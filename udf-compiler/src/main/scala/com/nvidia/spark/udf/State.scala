/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

package com.nvidia.spark.udf

import CatalystExpressionBuilder.simplify
import javassist.CtClass

import org.apache.spark.sql.catalyst.expressions.{Expression, If, Literal, Or}

/**
 * State is used as the main representation of block state, as we walk the bytecode.
 *
 * Given a set of instructions, we will use State variables to track what happens to the stack.
 *
 * The final State generated is later used to simplify expressions.
 *
 * Example 1:
 * {
 * return 0
 * }
 *
 * This is in java byte code:
 * iconst 0
 * ireturn
 *
 * iconst 0  => pushes 0 to stack
 * ireturn   => pops and returns
 *
 * Example 2:
 * {
 * return 2 + 2 + 1
 * }
 *
 * 1) State(locals, empty stack, no condition, expr?) // expr ==it is no expression
 *
 * iconst 2
 * NOTE: 2 is literal here so is 1
 * 2) State(locals, 2::Nil, no condition, expr is still empty)
 *
 * iconst 2
 * 3) State(locals, 2::2::Nil, no condiiton...)
 *
 * iadd (pop 2 and 2 + push 4 into stack)
 * 4) State(locals, Add(2, 2)::Nil, no condition, expr is still empoty)
 *
 * iconst 1
 * 5) State(locals, 1::Add(2,2)::Nil, ..)
 *
 * iadd (pop 1 and 4 + push 5 into sack)
 * 6) Add(1, Add(2,2)) :: Nil
 *
 * ireturn (pop 5 from stack)
 * 7) return Add...
 *
 * State == Add
 * stack == lhs/rhs
 *
 * @param locals
 * @param stack
 * @param cond
 * @param expr
 */
case class State(locals: Array[Expression],
    stack: List[Expression] = List(),
    cond: Expression = Literal.TrueLiteral,
    expr: Option[Expression] = None) {

  def merge(that: Option[State]): State = {
    that.fold(this) { s =>
      val combine: ((Expression, Expression)) =>
          Expression = {
        case (l1, l2) => simplify(If(cond, l1, l2))
      }
      // At the end of the compliation, the expression at the top of stack is
      // returned, which must have all the conditionals embedded, if the
      // bytecode had any conditional.  For this reason, we apply combine to
      // each element in the stack and locals.
      s.copy(locals = locals.zip(s.locals).map(combine),
        stack = stack.zip(s.stack).map(combine),
        // The combined state is for the cases s.cond is met or cond
        // is met, hence or.
        cond = simplify(Or(s.cond, cond)))
    }
  }

  override def toString: String = {
    s"State(locals=[${printExpressions(locals)}], stack=[${printExpressions(stack)}], " +
        s"cond=[${printExpressions(Seq(cond))}], expr=[${expr.map(e => e.toString())}])"
  }

  private def printExpressions(expressions: Iterable[Expression]): String = {
    if (expressions == null) {
      "NULL"
    } else {
      expressions.map(e => if (e == null) {
        "NULL"
      } else {
        e.toString()
      }).mkString(", ")
    }
  }
}

object State {
  def makeStartingState(lambdaReflection: LambdaReflection,
      children: Seq[Expression]): State = {
    val max = lambdaReflection.maxLocals
    val params: Seq[(CtClass, Expression)] = lambdaReflection.parameters.view.zip(children)
    val (locals, _) = params.foldLeft((new Array[Expression](max), 0)) { (l, p) =>
      val (locals: Array[Expression], index: Int) = l
      val (param: CtClass, argExp: Expression) = p

      val newIndex = if (param == CtClass.doubleType || param == CtClass.longType) {
        // Long and Double occupies two slots in the local variable array.
        // See https://docs.oracle.com/javase/specs/jvms/se8/html/jvms-2.html#jvms-2.6.1
        index + 2
      } else {
        index + 1
      }

      (locals.updated(index, argExp), newIndex)
    }
    State(locals)
  }
}
