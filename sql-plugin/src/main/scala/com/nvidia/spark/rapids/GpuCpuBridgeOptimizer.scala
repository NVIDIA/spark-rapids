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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Expression, ExprId, Literal}
import org.apache.spark.sql.execution.ScalarSubquery

/**
 * Optimizer to make decisions like, which expressions should we run on the CPU vs the GPU
 * and combining cpu expressions into an expression tree.
 */
object GpuCpuBridgeOptimizer extends Logging {

  def checkAndOptimizeExpressionMetas(exprs: Seq[BaseExprMeta[_]]): Unit = {
    if (exprs.nonEmpty && exprs.head.conf.isCpuBridgeEnabled) {
      exprs.foreach { child =>
        if (!child.canExprTreeBeReplaced && canRunOnCpuOrGpuRecursively(child)) {
          // Minimize data movement for this expression tree assuming the consumer is on GPU
          optimizeByMinimizingMovement(child)
        }
      }
    }
  }

  private def canRunOnCpuOrGpuRecursively(expr: BaseExprMeta[_]): Boolean = {
    if (!expr.canThisBeReplaced && !expr.canMoveToCpuBridge) {
      false
    } else {
      expr.childExprs.forall(canRunOnCpuOrGpuRecursively)
    }
  }

  /**
   * Determine whether an expression is effectively scalar-like for the purpose of
   * CPU/GPU placement. An expression is scalar-like if it is a `Literal`, a `ScalarSubquery`,
   * or if all of its inputs are scalar-like (yielding a scalar result as well).
   * Zero-arity non-literal expressions are not treated as scalar-like.
   */
  def isScalarLike(exprMeta: BaseExprMeta[_]): Boolean = {
    exprMeta.wrapped match {
      case _: Literal => true
      case _: ScalarSubquery => true
      case _ =>
        exprMeta.childExprs.nonEmpty && exprMeta.childExprs.forall(isScalarLike)
    }
  }

  /** True if this meta wraps a data leaf that identifies a column input. */
  def isDataLeaf(exprMeta: BaseExprMeta[_]): Boolean = {
    exprMeta.wrapped match {
      case _: AttributeReference => true
      case BoundReference(_, _, _) => true
      case _ => false
    }
  }

  // ------------------------------
  // Cost-based placement optimizer
  // ------------------------------

  private sealed trait ExecSide
  private case object OnGpu extends ExecSide
  private case object OnCpu extends ExecSide

  private sealed trait InputKey
  private case class AttrKey(exprId: ExprId) extends InputKey
  private case class BoundKey(ordinal: Int) extends InputKey

  private case class PlacementCost(gpuCost: Int, cpuCost: Int, cpuRequiredLeaves: Set[InputKey])

  /** Identify an input key for leaves that represent data-carrying inputs. */
  private def leafInputKey(expr: Expression): Option[InputKey] = expr match {
    case ar: AttributeReference => Some(AttrKey(ar.exprId))
    case BoundReference(ordinal, _, _) => Some(BoundKey(ordinal))
    case _ => None
  }

  /**
   * Optimize an expression tree by minimizing estimated data movement, using a
   * bottom-up dynamic programming approach. We assume the parent/consumer is running
   * on the GPU at the root, so we include the cost of moving the root's output when
   * placed on CPU. If we need to add expression costs in the future, we can do so.
   */
  def optimizeByMinimizingMovement(root: BaseExprMeta[_]): Unit = {
    val startTimeNs = System.nanoTime()
    val costCache = scala.collection.mutable.HashMap[BaseExprMeta[_], PlacementCost]()
    var subsetEvaluations: Long = 0L

    def sideName(s: ExecSide): String = s match {
      case OnGpu => "GPU"
      case OnCpu => "CPU"
    }

    // Enumerate all child subsets to compute exact CPU placement cost and leaves
    def chooseCpuSubset(
        parent: BaseExprMeta[_],
        childMetas: Seq[BaseExprMeta[_]],
        childCosts: Seq[PlacementCost]): (Set[Int], Int, Set[InputKey]) = {
      val n = childMetas.length
      val heuristicThreshold = 12
      if (n > heuristicThreshold) {
        // Heuristic fast path for large fan-out: independent per-child choice.
        // Ties favor GPU.
        var sumCpu = 0
        var sumGpu = 0
        var leaves: Set[InputKey] = Set.empty
        var subset: Set[Int] = Set.empty
        var valid = true
        var i = 0
        while (i < n && valid) {
          val meta = childMetas(i)
          val costs = childCosts(i)
          val gpuAllowed = costs.gpuCost != Int.MaxValue
          val cpuAllowed = costs.cpuCost != Int.MaxValue
          if (!gpuAllowed && !cpuAllowed) {
            valid = false
          } else if (!gpuAllowed) {
            // must choose CPU
            subset += i
            sumCpu += costs.cpuCost
            leaves ++= costs.cpuRequiredLeaves
          } else if (!cpuAllowed) {
            // must choose GPU
            val moveOut = if (isScalarLike(meta)) 0 else 1
            sumGpu += costs.gpuCost + moveOut
          } else {
            val moveOut = if (isScalarLike(meta)) 0 else 1
            val gpuAlt = costs.gpuCost + moveOut
            if (costs.cpuCost < gpuAlt) {
              subset += i
              sumCpu += costs.cpuCost
              leaves ++= costs.cpuRequiredLeaves
            } else {
              // prefer GPU on ties
              sumGpu += gpuAlt
            }
          }
          i += 1
        }
        val total = if (valid) sumCpu + sumGpu else Int.MaxValue
        logDebug(s"Heuristic subset for ${parent.wrapped.getClass.getSimpleName}: " +
          s"cost=$total leaves=${leaves.size} chosen=${subset.mkString(",")}")
        return (subset, total, leaves)
      }
      var bestCost = Int.MaxValue
      var bestLeaves: Set[InputKey] = Set.empty
      var bestSubset: Set[Int] = Set.empty

      var mask = 0
      val maxMask = 1 << n
      while (mask < maxMask) {
        subsetEvaluations += 1
        var sumCpu = 0
        var sumGpu = 0
        var leaves = Set.empty[InputKey]
        var valid = true
        var i = 0
        while (i < n && valid) {
          val meta = childMetas(i)
          val costs = childCosts(i)
          val onCpu = (mask & (1 << i)) != 0
          if (onCpu) {
            if (costs.cpuCost == Int.MaxValue) {
              valid = false
            } else {
              sumCpu += costs.cpuCost
              leaves ++= costs.cpuRequiredLeaves
            }
          } else {
            if (costs.gpuCost == Int.MaxValue) {
              valid = false
            } else {
              val moveOut = if (isScalarLike(meta)) 0 else 1
              sumGpu += costs.gpuCost + moveOut
            }
          }
          i += 1
        }
        if (valid) {
          // IMPORTANT: Do NOT include leaves.size here; we charge import cost only at the
          // CPU region boundary of the current parent, not per child subtree. This prevents
          // double-charging when merging child CPU regions into a parent CPU region.
          val total = sumCpu + sumGpu

          // Log this subset
          val chosenStr = {
            val b = new StringBuilder
            var j = 0
            var first = true
            while (j < n) {
              if ((mask & (1 << j)) != 0) {
                if (!first) b.append(',')
                b.append(j)
                first = false
              }
              j += 1
            }
            b.toString
          }
          logDebug(s"Subset mask=$mask [$chosenStr] sumCpu=$sumCpu sumGpu=$sumGpu " + 
            s"leaves=${leaves.size} total=$total")
          // Tie-breaking policy: favor GPU on ties → prefer fewer children on CPU.
          // Implement by preferring smaller chosen subset size on equal total cost.
          val chosenSize = java.lang.Integer.bitCount(mask)
          val bestSize = java.lang.Integer.bitCount({
            var acc = 0; bestSubset.foreach(i => acc |= (1 << i)); acc
          })
          val takeThis = (total < bestCost) ||
                         (total == bestCost && 
                           (chosenSize < bestSize || 
                             (chosenSize == bestSize && 
                               leaves.size < bestLeaves.size)))
          if (takeThis) {
            bestCost = total
            bestLeaves = leaves
            // materialize subset indices
            var chosen: Set[Int] = Set.empty
            var j = 0
            while (j < n) {
              if ((mask & (1 << j)) != 0) chosen += j
              j += 1
            }
            bestSubset = chosen

            logDebug(s"New best subset mask=$mask cost=$bestCost leaves=${bestLeaves.size} " + 
              s"chosen=${bestSubset.mkString(",")}")
          }
        }
        mask += 1
      }
      logError(s"Best subset for ${parent.wrapped.getClass.getSimpleName}: " + 
        s"cost=$bestCost leaves=${bestLeaves.size} chosen=${bestSubset.mkString(",")}")
      (bestSubset, bestCost, bestLeaves)
    }

    def computeCosts(expr: BaseExprMeta[_]): PlacementCost = {
      costCache.getOrElseUpdate(expr, {
        // Base case: data-carrying leaf input (AttributeReference/BoundReference)
        val maybeLeaf: Option[PlacementCost] = expr.wrapped match {
          case e: Expression =>
            leafInputKey(e).map { key =>
              // Conceptually placeable on either side at zero subtree cost; when placed on
              // CPU, record the input as a required leaf so we can transfer it once per
              // CPU region.
              val pc = PlacementCost(
                gpuCost = 0,
                cpuCost = 0,
                cpuRequiredLeaves = Set[InputKey](key))
              logDebug(s"Cost[leaf] ${expr.wrapped.getClass.getSimpleName}: gpu=0 cpu=0 " +
                s"leaves=${pc.cpuRequiredLeaves.size}")
              pc
            }
          case _ => None
        }
        maybeLeaf.getOrElse {
          // Compute child costs first
          val childCosts = expr.childExprs.map(computeCosts)

          val gpuAllowed = expr.canThisBeReplaced
          val cpuAllowed = expr.canMoveToCpuBridge

          // Cost if this node runs on GPU
          val gpuTotal: Int = if (gpuAllowed) {
            expr.childExprs.zip(childCosts).map { case (childMeta, costs) =>
              // Choose child side to minimize (child cost + edge move of child's output if needed)
              val childGpu = costs.gpuCost
              val childCpu = costs.cpuCost
              val moveCpuChildOutput = if (isScalarLike(childMeta)) 0 else 1
              val cpuChoice =
                if (childCpu == Int.MaxValue) Int.MaxValue else childCpu + moveCpuChildOutput
              val gpuChoice = childGpu
              Math.min(cpuChoice, gpuChoice)
            }.sum
          } else Int.MaxValue

          // Exact CPU cost via subset enumeration
          val (_, cpuTotal, cpuLeaves): (Set[Int], Int, Set[InputKey]) = if (cpuAllowed) {
            chooseCpuSubset(expr, expr.childExprs, childCosts)
          } else (Set.empty[Int], Int.MaxValue, Set.empty[InputKey])

          val result = PlacementCost(gpuTotal, cpuTotal, cpuLeaves)
          logDebug(
            s"Cost ${expr.wrapped.getClass.getSimpleName}: gpuAllowed=$gpuAllowed " +
            s"cpuAllowed=$cpuAllowed gpuTotal=$gpuTotal cpuTotal=$cpuTotal " +
            s"cpuLeaves=${cpuLeaves.size}")
          result
        }
      })
    }

    // Compute subtree costs
    val rootCosts = computeCosts(root)

    // Root consumer is on GPU → include edge penalty for root output if on CPU
    val rootCpuWithOutputMove = if (rootCosts.cpuCost == Int.MaxValue) Int.MaxValue
      else rootCosts.cpuCost + rootCosts.cpuRequiredLeaves.size + (if (isScalarLike(root)) 0 else 1)
    val rootGpuWithOutputMove = rootCosts.gpuCost // output already on GPU; no move

    val chooseRootSide: ExecSide = {
      if (rootGpuWithOutputMove != Int.MaxValue &&
          rootGpuWithOutputMove <= rootCpuWithOutputMove) {
        OnGpu
      } else {
        OnCpu
      }
    }

    logDebug(
      s"Root costs: gpu=$rootGpuWithOutputMove cpu=$rootCpuWithOutputMove " +
      s"chosen=${sideName(chooseRootSide)}")

    // Second pass: assign sides to minimize costs and apply moveToCpuBridge where needed
    def assignSides(expr: BaseExprMeta[_], chosenSide: ExecSide): Unit = {
      // Place current node
      chosenSide match {
        case OnCpu if expr.canMoveToCpuBridge && !expr.willUseGpuCpuBridge => expr.moveToCpuBridge()
        case _ => // keep on GPU or already on CPU bridge
      }
      logDebug(s"Place ${expr.wrapped.getClass.getSimpleName} on ${sideName(chosenSide)}")

      // Place children to minimize (childCost + edge penalty)
      val childCosts = expr.childExprs.map { child =>
        costCache.getOrElse(child, computeCosts(child))
      }

      chosenSide match {
        case OnGpu =>
          expr.childExprs.zip(childCosts).foreach { case (childMeta, costs) =>
            val moveCpuChildOutput = if (isScalarLike(childMeta)) 0 else 1
            val gpuCandidate = if (costs.gpuCost == Int.MaxValue) Int.MaxValue
              else costs.gpuCost
            val cpuCandidate = if (costs.cpuCost == Int.MaxValue) Int.MaxValue
              else costs.cpuCost + moveCpuChildOutput

            val childSide =
              if (gpuCandidate <= cpuCandidate || cpuCandidate == Int.MaxValue) OnGpu else OnCpu

            logDebug(
              s"Child ${childMeta.wrapped.getClass.getSimpleName}: gpuCand=$gpuCandidate " +
              s"cpuCand=$cpuCandidate choose=${sideName(childSide)} " +
              s"parent=${sideName(chosenSide)}")

            assignSides(childMeta, childSide)
          }
        case OnCpu =>
          // Use exact best subset assignment under CPU parent
          val (subset, localTotal, leavesForSubset) = 
            chooseCpuSubset(expr, expr.childExprs, childCosts)
          val effectiveTotal = if (localTotal == Int.MaxValue) Int.MaxValue
            else localTotal + leavesForSubset.size
          logError(s"CPU parent assign: subset=${subset.mkString(",")} localTotal=$localTotal " +
            s"leaves=${leavesForSubset.size} effectiveWithImports=$effectiveTotal")
          expr.childExprs.zipWithIndex.foreach { case (childMeta, idx) =>
            // Force scalar-like children to co-locate with CPU parent
            val side = if (isScalarLike(childMeta) || subset.contains(idx)) OnCpu else OnGpu
            logDebug(s"Child ${childMeta.wrapped.getClass.getSimpleName}: " + 
              s"choose=${sideName(side)} parent=CPU")
            assignSides(childMeta, side)
          }
      }
    }

    assignSides(root, chooseRootSide)

    val elapsedMs = (System.nanoTime() - startTimeNs) / 1000000.0
    // Compute max depth of the expression meta tree (root counts as 1)
    def maxDepth(meta: BaseExprMeta[_]): Int =
      if (meta.childExprs.isEmpty) 1 else 1 + meta.childExprs.map(maxDepth).max
      
    logError(s"optimizeByMinimizingMovement: nodes=${costCache.size} " +
      s"depth=${maxDepth(root)} " +
      s"subsets=$subsetEvaluations timeMs=$elapsedMs")
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