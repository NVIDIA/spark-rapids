/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression}

/**
 * Optimizer to make decisions like, which expressions should we run on the CPU vs the GPU
 * and combining cpu expressions into an expression tree.
 */
object GpuCpuBridgeOptimizer extends Logging {

  def checkAndOptimizeExpressionMetas(exprs: Seq[BaseExprMeta[_]]): Unit = {
    if (exprs.nonEmpty && exprs.head.conf.isCpuBridgeEnabled) {
      exprs.foreach { child =>
        if (!child.canExprTreeBeReplaced && canRunOnCpuOrGpuRecursively(child)) {
          // Then we want to move some things to the CPU as needed
          moveToCpuIfNeededRecursively(child, wasParentReplaced = false, 0)
        }
      }
    }
  }

  private def canRunOnCpuOrGpuRecursively(expr: BaseExprMeta[_]): Boolean = {
    if (!expr.canThisBeReplaced && !expr.canMoveToCpuBridge()) {
      false
    } else {
      expr.childExprs.forall(canRunOnCpuOrGpuRecursively)
    }
  }

  /**
   * Move an expression to the GPU/CPU bridge if it has to or if it would be
   * less data movement if it did run on the CPU, this means that it has more inputs
   * and outputs on the CPU than the GPU.
   * @param expr the expression to possibly move
   * @param wasParentReplaced will its output be on the CPU
   * @return true if this was placed on the CPU bridge else false
   */
  private def moveToCpuIfNeededRecursively(expr: BaseExprMeta[_],
                                           wasParentReplaced: Boolean,
                                           indent: Int): Boolean = {
    if (expr.willUseGpuCpuBridge) {
      // Some expression trees can be reused. If we are here, then we don't need to go any
      // deeper
      true
    } else if (!expr.canThisBeReplaced && expr.canMoveToCpuBridge()) {
      expr.childExprs.foreach { child =>
        moveToCpuIfNeededRecursively(child, wasParentReplaced = true, indent + 1)
      }
      expr.moveToCpuBridge()
      true
    } else if (expr.canThisBeReplaced && expr.canMoveToCpuBridge()) {
      // We need to check the cost of moving this (data movement only for now)
      // But we don't know if we are going to move until we know our children will
      // But they need to know if we will before they can tell, so just assume we
      // will not move and the children may be wrong if that is incorrect, but
      // we want to prefer the GPU for execution if possible
      val maxPossibleCost: Int = expr.childExprs.length + 1
      // This is for the output
      val parentCost = if (wasParentReplaced) 1 else 0
      val childrenMovedToCpu = expr.childExprs.map { child =>
        moveToCpuIfNeededRecursively(child, wasParentReplaced = false, indent + 1)
      }
      val costToStayOnGPU = childrenMovedToCpu.count(b => b) + parentCost
      val costToMoveDataToCPU = maxPossibleCost - costToStayOnGPU

      // prefer the GPU if things are equal
      if (costToStayOnGPU > costToMoveDataToCPU) {
        expr.moveToCpuBridge()
        true
      } else {
        false
      }
    } else {
      // This can be replaced, but cannot run on the bridge, so check its children
      expr.childExprs.foreach { child =>
        moveToCpuIfNeededRecursively(child, wasParentReplaced = false, indent + 1)
      }
      false
    }
  }

  /**
   * Optimize a GPU expression tree by merging adjacent CPU bridge expressions.
   * This should be called after initial GPU/CPU tagging but before execution.
   */
  def optimizeBridgeExpressions(expr: GpuExpression): GpuExpression = {
    expr match {
      case bridge: GpuCpuBridgeExpression =>
        // Check if any of this bridge's inputs are also bridge expressions
        val hasBridgeInputs = bridge.gpuInputs.exists(_.isInstanceOf[GpuCpuBridgeExpression])
        
        if (hasBridgeInputs) {
          mergeBridgeExpressions(bridge)
        } else {
          bridge
        }
      case other => 
        // For non-bridge expressions, recursively optimize children
        // This is a simplified approach - in a full implementation we'd handle all expression types
        other
    }
  }

  /**
   * Merge a bridge expression with its bridge input expressions.
   * Handles multiple bridge inputs by flattening GPU inputs and remapping BoundReferences.
   * 
   * @param parentBridge The outer bridge expression
   * @return A single merged bridge expression
   */
  private def mergeBridgeExpressions(
      parentBridge: GpuCpuBridgeExpression): GpuCpuBridgeExpression = {
    
    // Collect all GPU inputs and create mapping for BoundReference remapping
    val (flattenedGpuInputs, inputMapping) = flattenBridgeInputs(parentBridge.gpuInputs)
    
    // Rewrite the parent CPU expression to substitute bridge expressions and 
    // remap BoundReferences
    val mergedCpuExpr = rewriteCpuExpression(parentBridge.cpuExpression, 
      parentBridge.gpuInputs, inputMapping)
    
    GpuCpuBridgeExpression(
      gpuInputs = flattenedGpuInputs,
      cpuExpression = mergedCpuExpr,
      outputDataType = parentBridge.dataType,
      outputNullable = parentBridge.nullable,
      codegenEnabled = parentBridge.codegenEnabled
    )
  }
  
  /**
   * Flatten bridge inputs by collecting all non-bridge GPU inputs and creating a mapping
   * from original input positions to final flattened positions. Includes deduplication
   * to reduce memory transfers and code size.
   * 
   * @param inputs The original bridge inputs (mix of bridges and GPU expressions)
   * @return (flattened GPU inputs, mapping from input index to position range in flattened inputs)
   */
  private def flattenBridgeInputs(
      inputs: Seq[Expression]): (Seq[Expression], Map[Int, InputMapping]) = {
    
    val flattenedInputs = scala.collection.mutable.ListBuffer[Expression]()
    val inputMapping = scala.collection.mutable.Map[Int, InputMapping]()
    
    inputs.zipWithIndex.foreach { case (input, originalIndex) =>
      input match {
        case bridge: GpuCpuBridgeExpression =>
          // Recursively flatten this bridge's inputs
          val nestedResult =  flattenBridgeInputs(bridge.gpuInputs)
          val nestedInputs = nestedResult._1
          val startIndex = flattenedInputs.length
          flattenedInputs ++= nestedInputs
          
          if (nestedInputs.nonEmpty) {
            val endIndex = flattenedInputs.length - 1
            inputMapping(originalIndex) = BridgeInputMapping(bridge, startIndex, endIndex)
          } else {
            // Bridge has no GPU inputs - use invalid range to indicate this
            inputMapping(originalIndex) = BridgeInputMapping(bridge, startIndex, startIndex - 1)
          }
          
        case gpuExpr =>
          // Regular GPU expression - add directly
          val index = flattenedInputs.length
          flattenedInputs += gpuExpr
          inputMapping(originalIndex) = DirectInputMapping(index)
      }
    }
    
    // Apply deduplication to the flattened inputs
    val (deduplicatedInputs, deduplicationMapping) = 
      deduplicateFlattenedInputs(flattenedInputs.toSeq)
    
    // Update input mappings to account for deduplication
    val finalInputMapping = inputMapping.map { case (originalIndex, mapping) =>
      originalIndex -> updateMappingAfterDeduplication(mapping, deduplicationMapping)
    }.toMap
    
    (deduplicatedInputs, finalInputMapping)
  }
  
  /**
   * Deduplicate flattened GPU inputs using semantic equality.
   * @param flattenedInputs All flattened GPU inputs (may contain duplicates)
   * @return (deduplicated inputs, mapping from old indices to new indices)
   */
  private def deduplicateFlattenedInputs(
      flattenedInputs: Seq[Expression]): (Seq[Expression], Map[Int, Int]) = {
    
    import org.apache.spark.sql.rapids.catalyst.expressions.GpuExpressionEquals
    
    val deduplicatedInputs = scala.collection.mutable.ListBuffer[Expression]()
    val seenExpressions = scala.collection.mutable.Map[GpuExpressionEquals, Int]()
    val deduplicationMapping = scala.collection.mutable.Map[Int, Int]()
        
    flattenedInputs.zipWithIndex.foreach { case (expr, oldIndex) =>
      val exprWrapper = GpuExpressionEquals(expr)
      seenExpressions.get(exprWrapper) match {
        case Some(existingIndex) =>
          // This expression is a duplicate - map to existing index
          deduplicationMapping(oldIndex) = existingIndex
        case None =>
          // This is a new unique expression - add it
          val newIndex = deduplicatedInputs.length
          deduplicatedInputs += expr
          seenExpressions(exprWrapper) = newIndex
          deduplicationMapping(oldIndex) = newIndex
      }
    }
    
    (deduplicatedInputs.toSeq, deduplicationMapping.toMap)
  }
  
  /**
   * Update an InputMapping to account for deduplication of the flattened inputs.
   * @param mapping The original mapping
   * @param deduplicationMapping Mapping from old flattened indices to deduplicated indices
   * @return Updated mapping with new indices
   */
  private def updateMappingAfterDeduplication(
      mapping: InputMapping,
      deduplicationMapping: Map[Int, Int]): InputMapping = {
    
    mapping match {
      case DirectInputMapping(oldIndex) =>
        DirectInputMapping(deduplicationMapping(oldIndex))
      case BridgeInputMapping(bridge, oldStartIndex, oldEndIndex) =>
        // Find the new start and end indices after deduplication
        if (oldStartIndex <= oldEndIndex) {
          val newIndices = (oldStartIndex to oldEndIndex).map(deduplicationMapping(_))
          val newStartIndex = newIndices.min
          val newEndIndex = newIndices.max
          BridgeInputMapping(bridge, newStartIndex, newEndIndex)
        } else {
          // Handle case where bridge has no GPU inputs (empty range)
          // This can happen when a bridge expression has only CPU expressions as inputs
          // No deduplication mapping needed since there are no GPU inputs
          BridgeInputMapping(bridge, oldStartIndex, oldEndIndex)
        }
    }
  }
  
  /**
   * Rewrite a CPU expression by substituting bridge expressions and remapping BoundReferences.
   * 
   * @param cpuExpr The CPU expression to rewrite
   * @param originalInputs The original inputs that the CPU expression references
   * @param inputMapping Mapping from original input positions to flattened positions
   * @return The rewritten CPU expression with proper BoundReference mappings
   */
  private def rewriteCpuExpression(
      cpuExpr: Expression,
      originalInputs: Seq[Expression],
      inputMapping: Map[Int, InputMapping]): Expression = {
    
    cpuExpr.transformUp {
      case BoundReference(ordinal, dataType, nullable) =>
        inputMapping.get(ordinal) match {
          case Some(BridgeInputMapping(bridge, startIndex, _)) =>
            // This BoundReference points to a bridge - substitute the bridge's CPU expression
            // and remap its BoundReferences to point to the correct positions in flattened inputs
            remapBoundReferences(bridge.cpuExpression, startIndex)
            
          case Some(DirectInputMapping(newIndex)) =>
            // This BoundReference points to a direct GPU input - update the index
            BoundReference(newIndex, dataType, nullable)
            
          case None =>
            throw new IllegalStateException(
                s"Invalid BoundReference ordinal $ordinal during bridge merging")
        }
    }
  }
  
  /**
   * Remap all BoundReferences in a CPU expression by adding an offset.
   * Used when substituting a bridge's CPU expression into a larger merged expression.
   * 
   * @param expr The CPU expression to remap
   * @param offset The offset to add to all BoundReference ordinals
   * @return The expression with remapped BoundReferences
   */
  private def remapBoundReferences(
      expr: Expression,
      offset: Int): Expression = {
    
    expr.transformUp {
      case BoundReference(ordinal, dataType, nullable) =>
        BoundReference(ordinal + offset, dataType, nullable)
    }
  }
  
  // Helper classes for input mapping
  sealed trait InputMapping
  case class DirectInputMapping(newIndex: Int) extends InputMapping
  case class BridgeInputMapping(bridge: GpuCpuBridgeExpression, 
    startIndex: Int, endIndex: Int) extends InputMapping
}