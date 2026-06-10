/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.nvidia

import java.lang.invoke.SerializedLambda

import org.apache.spark.sql.Column
import org.apache.spark.sql.api.java._
import org.apache.spark.util.Utils

trait DFUDF {
  def apply(input: Array[Column]): Column
}

class DFUDF0(val f: Function0[Column])
  extends UDF0[Any] with DFUDF {
  override def call(): Any = {
    throw new IllegalStateException("TODO better error message. This should have been replaced")
  }

  override def apply(input: Array[Column]): Column = {
    assert(input.length == 0)
    f()
  }
}

class DFUDF1(val f: Function1[Column, Column])
  extends UDF1[Any, Any] with DFUDF {
  override def call(t1: Any): Any = {
    throw new IllegalStateException("TODO better error message. This should have been replaced")
  }

  override def apply(input: Array[Column]): Column = {
    assert(input.length == 1)
    f(input(0))
  }
}

class DFUDF2(val f: Function2[Column, Column, Column])
  extends UDF2[Any, Any, Any] with DFUDF {
  override def call(t1: Any, t2: Any): Any = {
    throw new IllegalStateException("TODO better error message. This should have been replaced")
  }

  override def apply(input: Array[Column]): Column = {
    assert(input.length == 2)
    f(input(0), input(1))
  }
}

class DFUDF3(val f: Function3[Column, Column, Column, Column])
  extends UDF3[Any, Any, Any, Any] with DFUDF {
  override def call(t1: Any, t2: Any, t3: Any): Any = {
    throw new IllegalStateException("TODO better error message. This should have been replaced")
  }

  override def apply(input: Array[Column]): Column = {
    assert(input.length == 3)
    f(input(0), input(1), input(2))
  }
}

class DFUDF4(val f: Function4[Column, Column, Column, Column, Column])
  extends UDF4[Any, Any, Any, Any, Any] with DFUDF {
  override def call(t1: Any, t2: Any, t3: Any, t4: Any): Any = {
    throw new IllegalStateException("TODO better error message. This should have been replaced")
  }

  override def apply(input: Array[Column]): Column = {
    assert(input.length == 4)
    f(input(0), input(1), input(2), input(3))
  }
}

class DFUDF5(val f: Function5[Column, Column, Column, Column, Column, Column])
  extends UDF5[Any, Any, Any, Any, Any, Any] with DFUDF {
  override def call(t1: Any, t2: Any, t3: Any, t4: Any, t5: Any): Any = {
    throw new IllegalStateException("TODO better error message. This should have been replaced")
  }

  override def apply(input: Array[Column]): Column = {
    assert(input.length == 5)
    f(input(0), input(1), input(2), input(3), input(4))
  }
}

class DFUDF6(val f: Function6[Column, Column, Column, Column, Column, Column, Column])
  extends UDF6[Any, Any, Any, Any, Any, Any, Any] with DFUDF {
  override def call(t1: Any, t2: Any, t3: Any, t4: Any, t5: Any, t6: Any): Any = {
    throw new IllegalStateException("TODO better error message. This should have been replaced")
  }

  override def apply(input: Array[Column]): Column = {
    assert(input.length == 6)
    f(input(0), input(1), input(2), input(3), input(4), input(5))
  }
}

class DFUDF7(val f: Function7[Column, Column, Column, Column, Column, Column, Column, Column])
  extends UDF7[Any, Any, Any, Any, Any, Any, Any, Any] with DFUDF {
  override def call(t1: Any, t2: Any, t3: Any, t4: Any, t5: Any, t6: Any, t7: Any): Any = {
    throw new IllegalStateException("TODO better error message. This should have been replaced")
  }

  override def apply(input: Array[Column]): Column = {
    assert(input.length == 7)
    f(input(0), input(1), input(2), input(3), input(4), input(5), input(6))
  }
}

class DFUDF8(val f: Function8[Column, Column, Column, Column, Column, Column, Column, Column,
  Column])
  extends UDF8[Any, Any, Any, Any, Any, Any, Any, Any, Any] with DFUDF {
  override def call(t1: Any, t2: Any, t3: Any, t4: Any, t5: Any, t6: Any, t7: Any, t8: Any): Any = {
    throw new IllegalStateException("TODO better error message. This should have been replaced")
  }

  override def apply(input: Array[Column]): Column = {
    assert(input.length == 8)
    f(input(0), input(1), input(2), input(3), input(4), input(5), input(6), input(7))
  }
}

class DFUDF9(val f: Function9[Column, Column, Column, Column, Column, Column, Column, Column,
  Column, Column])
  extends UDF9[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any] with DFUDF {
  override def call(t1: Any, t2: Any, t3: Any, t4: Any, t5: Any, t6: Any, t7: Any, t8: Any,
                    t9: Any): Any = {
    throw new IllegalStateException("TODO better error message. This should have been replaced")
  }

  override def apply(input: Array[Column]): Column = {
    assert(input.length == 9)
    f(input(0), input(1), input(2), input(3), input(4), input(5), input(6), input(7), input(8))
  }
}

class DFUDF10(val f: Function10[Column, Column, Column, Column, Column, Column, Column, Column,
  Column, Column, Column])
  extends UDF10[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any] with DFUDF {
  override def call(t1: Any, t2: Any, t3: Any, t4: Any, t5: Any, t6: Any, t7: Any, t8: Any,
                    t9: Any, t10: Any): Any = {
    throw new IllegalStateException("TODO better error message. This should have been replaced")
  }

  override def apply(input: Array[Column]): Column = {
    assert(input.length == 10)
    f(input(0), input(1), input(2), input(3), input(4), input(5), input(6), input(7), input(8),
      input(9))
  }
}

class JDFUDF0(val f: UDF0[Column])
  extends UDF0[Any] with DFUDF {
  override def call(): Any = {
    throw new IllegalStateException("TODO better error message. This should have been replaced")
  }

  override def apply(input: Array[Column]): Column = {
    assert(input.length == 0)
    f.call()
  }
}

class JDFUDF1(val f: UDF1[Column, Column])
  extends UDF1[Any, Any] with DFUDF {
  override def call(t1: Any): Any = {
    throw new IllegalStateException("TODO better error message. This should have been replaced")
  }

  override def apply(input: Array[Column]): Column = {
    assert(input.length == 1)
    f.call(input(0))
  }
}

class JDFUDF2(val f: UDF2[Column, Column, Column])
  extends UDF2[Any, Any, Any] with DFUDF {
  override def call(t1: Any, t2: Any): Any = {
    throw new IllegalStateException("TODO better error message. This should have been replaced")
  }

  override def apply(input: Array[Column]): Column = {
    assert(input.length == 2)
    f.call(input(0), input(1))
  }
}

class JDFUDF3(val f: UDF3[Column, Column, Column, Column])
  extends UDF3[Any, Any, Any, Any] with DFUDF {
  override def call(t1: Any, t2: Any, t3: Any): Any = {
    throw new IllegalStateException("TODO better error message. This should have been replaced")
  }

  override def apply(input: Array[Column]): Column = {
    assert(input.length == 3)
    f.call(input(0), input(1), input(2))
  }
}

class JDFUDF4(val f: UDF4[Column, Column, Column, Column, Column])
  extends UDF4[Any, Any, Any, Any, Any] with DFUDF {
  override def call(t1: Any, t2: Any, t3: Any, t4: Any): Any = {
    throw new IllegalStateException("TODO better error message. This should have been replaced")
  }

  override def apply(input: Array[Column]): Column = {
    assert(input.length == 4)
    f.call(input(0), input(1), input(2), input(3))
  }
}

class JDFUDF5(val f: UDF5[Column, Column, Column, Column, Column, Column])
  extends UDF5[Any, Any, Any, Any, Any, Any] with DFUDF {
  override def call(t1: Any, t2: Any, t3: Any, t4: Any, t5: Any): Any = {
    throw new IllegalStateException("TODO better error message. This should have been replaced")
  }

  override def apply(input: Array[Column]): Column = {
    assert(input.length == 5)
    f.call(input(0), input(1), input(2), input(3), input(4))
  }
}

class JDFUDF6(val f: UDF6[Column, Column, Column, Column, Column, Column, Column])
  extends UDF6[Any, Any, Any, Any, Any, Any, Any] with DFUDF {
  override def call(t1: Any, t2: Any, t3: Any, t4: Any, t5: Any, t6: Any): Any = {
    throw new IllegalStateException("TODO better error message. This should have been replaced")
  }

  override def apply(input: Array[Column]): Column = {
    assert(input.length == 6)
    f.call(input(0), input(1), input(2), input(3), input(4), input(5))
  }
}

class JDFUDF7(val f: UDF7[Column, Column, Column, Column, Column, Column, Column, Column])
  extends UDF7[Any, Any, Any, Any, Any, Any, Any, Any] with DFUDF {
  override def call(t1: Any, t2: Any, t3: Any, t4: Any, t5: Any, t6: Any, t7: Any): Any = {
    throw new IllegalStateException("TODO better error message. This should have been replaced")
  }

  override def apply(input: Array[Column]): Column = {
    assert(input.length == 7)
    f.call(input(0), input(1), input(2), input(3), input(4), input(5), input(6))
  }
}

class JDFUDF8(val f: UDF8[Column, Column, Column, Column, Column, Column, Column, Column,
  Column])
  extends UDF8[Any, Any, Any, Any, Any, Any, Any, Any, Any] with DFUDF {
  override def call(t1: Any, t2: Any, t3: Any, t4: Any, t5: Any, t6: Any, t7: Any, t8: Any): Any = {
    throw new IllegalStateException("TODO better error message. This should have been replaced")
  }

  override def apply(input: Array[Column]): Column = {
    assert(input.length == 8)
    f.call(input(0), input(1), input(2), input(3), input(4), input(5), input(6), input(7))
  }
}

class JDFUDF9(val f: UDF9[Column, Column, Column, Column, Column, Column, Column, Column,
  Column, Column])
  extends UDF9[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any] with DFUDF {
  override def call(t1: Any, t2: Any, t3: Any, t4: Any, t5: Any, t6: Any, t7: Any, t8: Any,
                    t9: Any): Any = {
    throw new IllegalStateException("TODO better error message. This should have been replaced")
  }

  override def apply(input: Array[Column]): Column = {
    assert(input.length == 9)
    f.call(input(0), input(1), input(2), input(3), input(4), input(5), input(6), input(7), input(8))
  }
}

class JDFUDF10(val f: UDF10[Column, Column, Column, Column, Column, Column, Column, Column,
  Column, Column, Column])
  extends UDF10[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any] with DFUDF {
  override def call(t1: Any, t2: Any, t3: Any, t4: Any, t5: Any, t6: Any, t7: Any, t8: Any,
                    t9: Any, t10: Any): Any = {
    throw new IllegalStateException("TODO better error message. This should have been replaced")
  }

  override def apply(input: Array[Column]): Column = {
    assert(input.length == 10)
    f.call(input(0), input(1), input(2), input(3), input(4), input(5), input(6), input(7), input(8),
      input(9))
  }
}

object DFUDF {
  /**
   * Determine if the UDF function implements the DFUDF.
   */
  def getDFUDF(function: AnyRef): Option[DFUDF] = {
    function match {
      case f: DFUDF => Some(f)
      case f =>
        try {
          // This may be a lambda that Spark's UDFRegistration wrapped around a Java UDF instance.
          val clazz = f.getClass
          if (Utils.getSimpleName(clazz).toLowerCase().contains("lambda")) {
            // Try to find a `writeReplace` method, further indicating it is likely a lambda
            // instance, and invoke it to serialize the lambda. Once serialized, captured arguments
            // can be examined to locate the Java UDF instance.
            // Note this relies on implementation details of Spark's UDFRegistration class.
            val writeReplace = clazz.getDeclaredMethod("writeReplace")
            writeReplace.setAccessible(true)
            val serializedLambda = writeReplace.invoke(f).asInstanceOf[SerializedLambda]
            if (serializedLambda.getCapturedArgCount == 1) {
              serializedLambda.getCapturedArg(0) match {
                case c: DFUDF => Some(c)
                case _ => None
              }
            } else {
              None
            }
          } else {
            None
          }
        } catch {
          case _: ClassCastException | _: NoSuchMethodException | _: SecurityException => None
        }
    }
  }
}
