/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

package org.apache.spark.sql.tests.datagen

import java.math.{BigDecimal => JavaBigDecimal}
import java.sql.{Date, Timestamp}
import java.time.{Duration, Instant, LocalDate, LocalDateTime}
import java.util

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.BigDecimal.RoundingMode
import scala.util.Random

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, XXH64}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, DateTimeUtils}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.random.XORShiftRandom

/**
 * Holds a representation of the current row location.
 * @param rowNum the top level row number (starting at 0 but this might change)
 * @param subRows a path to the child rows in an array starting at 0 for each array.
 */
class RowLocation(val rowNum: Long, val subRows: Array[Int] = null) {
  def withNewChild(): RowLocation = {
    val newSubRows = if (subRows == null) {
      new Array[Int](1)
    } else {
      val tmp = new Array[Int](subRows.length + 1)
      subRows.copyToArray(tmp, 0, subRows.length)
      tmp
    }
    new RowLocation(rowNum, newSubRows)
  }

  def setLastChildIndex(index: Int): Unit =
    subRows(subRows.length - 1) = index

  /**
   * Hash the location into a single long value.
   */
  def hashLoc(seed: Long): Long = {
    var tmp = XXH64.hashLong(rowNum, seed)
    if (subRows != null) {
      var i = 0
      while (i < subRows.length) {
        tmp = XXH64.hashLong(subRows(i), tmp)
        i += 1
      }
    }
    tmp
  }
}

/**
 * Holds a representation of the current columns location. This is computed by walking
 * the tree. The path is depth first, but as we pass a new node we assign it an ID as
 * we go starting at 0. If this is a part of a correlated key group then the
 * correlatedKeyGroup will be set and the columnNum will be ignored when computing the
 * hash. This makes the generated data correlated for all column/child columns.
 * @param tableNum a unique ID for the table this is a part of.
 * @param columnNum the location of the column in the data being generated
 * @param correlatedKeyGroup the correlated key group this column is a part of, if any.
 */
case class ColumnLocation(tableNum: Int, columnNum: Int, correlatedKeyGroup: Option[Long] = None) {
  def forNextColumn(): ColumnLocation = ColumnLocation(tableNum, columnNum + 1)


  /**
   * Create a new ColumnLocation that is specifically for a given key group
   */
  def forCorrelatedKeyGroup(keyGroup: Long): ColumnLocation =
    ColumnLocation(tableNum, columnNum, Some(keyGroup))

  /**
   * Hash the location into a single long value.
   */
  lazy val hashLoc: Long = XXH64.hashLong(tableNum, correlatedKeyGroup.getOrElse(columnNum))
}

/**
 * Holds configuration for a given column, or sub-column.
 * @param columnLoc the location of the column
 * @param nullable are nulls supported in the output or not.
 * @param numRows the number of rows that this table is going to have, not necessarily the number
 *                that the column is going to have.
 * @param minSeed the minimum seed value allowed to be returned
 * @param maxSeed the maximum seed value allowed to be returned
 */
case class ColumnConf(columnLoc: ColumnLocation,
    nullable: Boolean,
    numTableRows: Long,
    minSeed: Long = Long.MinValue,
    maxSeed: Long = Long.MaxValue) {

  def forNextColumn(nullable: Boolean): ColumnConf =
    ColumnConf(columnLoc.forNextColumn(), nullable, numTableRows)

  /**
   * Create a new configuration based on this, but for a given correlated key group.
   */
  def forCorrelatedKeyGroup(correlatedKeyGroup: Long): ColumnConf = {
    ColumnConf(columnLoc.forCorrelatedKeyGroup(correlatedKeyGroup),
      nullable,
      numTableRows,
      minSeed,
      maxSeed)
  }

  /**
   * Create a new configuration based on this, but for a given seed range.
   */
  def forSeedRange(min: Long, max: Long): ColumnConf =
    ColumnConf(columnLoc, nullable, numTableRows, min, max)

  /**
   * Create a new configuration based on this, but for the null generator.
   */
  def forNulls: ColumnConf = {
    assert(nullable, "Should not get a conf for nulls from a non-nullable column conf")
    ColumnConf(columnLoc, nullable, numTableRows)
  }
}

/**
 * Provides a mapping between a location + configuration and a seed. This provides a random
 * looking mapping between a location and a seed that will be used to deterministically
 * generate data. This is also where we can change the distribution and cardinality of data.
 * A min/max seed allows for changes to the cardinality of values generated and the exact
 * mapping done can inject skew or various types of data distributions.
 */
trait LocationToSeedMapping extends Serializable {
  /**
   * Set the config for the column that should be used along with a min/max type seed range.
   * This will be called before apply is ever called.
   */
  def withColumnConf(colConf: ColumnConf): LocationToSeedMapping

  /**
   * Given a row location + the previously set config produce a seed to be used.
   * @param rowLoc the row location
   * @return the seed to use
   */
  def apply(rowLoc: RowLocation): Long
}

object LocationToSeedMapping {
  /**
   * Return a function that should remap the full range of long seed
   * (Long.MinValue to Long.MaxValue) to a new range as described by the conf in a way that
   * does not really impact the distribution. This is not 100% correct
   * because if the min/max is not the full range the mapping cannot always guarantee that
   * each option gets exactly the same probabilty of being produced up.
   * But it should be good enough.
   */
  def remapRangeFunc(colConf: ColumnConf): Long => Long = {
    val minSeed = colConf.minSeed
    val maxSeed = colConf.maxSeed
    if (minSeed == Long.MinValue && maxSeed == Long.MaxValue) {
      n => n
    } else {
      // We generate numbers between minSeed and maxSeed + 1, and then take the floor so that
      // values on the ends have the same range as those in the middle
      val scaleFactor = (BigDecimal(maxSeed) + 1 - minSeed) /
          (BigDecimal(Long.MaxValue) - Long.MinValue)
      val minBig = BigDecimal(Long.MinValue)
      n => {
        (((n - minBig) * scaleFactor) + minSeed).setScale(0, RoundingMode.FLOOR).toLong
      }
    }
  }
}

/**
 * The idea is that we have an address (`RowLocation`, `ColumnLocation`)
 * that will uniquely identify the location of a piece of data to be generated.
 * A `GeneratorFunction` should distinctly map this address + configs to a
 * generated value. The column location is computed from the schema + configs
 * and will be set by calling `setColumnConf` along with other configs before
 * the function is used to generate any data. The row locations is created as needed
 * and could be reused in between calls to `apply`. This should keep no
 * state in between calls to `apply`.
 *
 * But we also want to have control over various aspects of how the data
 * is generated. We want
 * 1. The data to appear random (pseudo random)
 * 2. The number of unique values (cardinality)
 * 3. The distribution of those values (skew/etc)
 *
 * To accomplish this we also provide a location to seed mapping through the
 * `setLocationToSeedMapping` API. This too is guaranteed to be called before
 * `apply` is called.
 */
trait GeneratorFunction extends Serializable {
  /**
   * Set a location mapping function. This will be called before apply is ever called.
   */
  def withLocationToSeedMapping(mapping: LocationToSeedMapping): GeneratorFunction

  /**
   * Set a new value range. This will be called if a value range is set by the user before
   * apply is called. If this generator does not support a value range, then an exception
   * should be thrown.
   */
  def withValueRange(min: Any, max: Any): GeneratorFunction

  /**
   * Set a LengthGeneratorFunction for this generator. Not all types need a length so this
   * should only be overridden if it is needed. This will be called before apply is called.
   */
  def withLengthGeneratorFunction(lengthGen: LengthGeneratorFunction): GeneratorFunction = {
    this
  }

  /**
   * Actually generate the data. The data should be based off of the mapping and the row location
   * passed in. The output data needs to correspond to something that Spark will be able
   * to understand properly as the datatype this generator is for. Because we use an expression
   * to call this, it needs to be one of the "internal" types that Spark expects.
   */
  def apply(rowLoc: RowLocation): Any
}

/**
 * We want a way to be able to insert nulls as needed. We don't treat nulls
 * the same as other values and they are generally off by default. This
 * becomes a wrapper around another GeneratorFunction and decides
 * if a null should be returned before calling into the other function or not.
 * This will only be put into places where the schema allows it to be, and
 * where the user has configured it to be.
 */
trait NullGeneratorFunction extends GeneratorFunction {
  /**
   * Create a new NullGeneratorFunction based on this one, but wrapping the passed in
   * generator. This will be called before apply is called.
   */
  def withWrapped(gen: GeneratorFunction): NullGeneratorFunction

  override def withValueRange(min: Any, max: Any): GeneratorFunction =
    throw new IllegalArgumentException("Null Generators should not have a value range set for them")

  /**
   * Create a new NullGeneratorFunction bases on this one, but with the given mapping.
   * This is guaranteed to be called before apply is called.
   */
  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): NullGeneratorFunction
}

/**
 * Used to generate the length of a String, Array, Map, or any other variable length data.
 * This is treated as a separate concern from the data generation because it is hard to
 * not mess up the cardinality using a naive approach to generating a length. If you just
 * use something like Random.nextInt(maxLen) then you can get skew because each length
 * has a different maximum cardinality possible. This causes skew. For example if you want
 * length between 0 and 1, then half of the rows generated will be length 0 and half
 * will be length 1. So half the data will be a single value.
 */
trait LengthGeneratorFunction {
  def withLocationToSeedMapping(mapping: LocationToSeedMapping): LengthGeneratorFunction
  def apply(rowLoc: RowLocation): Int
}

/**
 * Just generate all of the data with a single length. This is the simplest way to avoid
 * skew because of the different possible cardinality for the different lengths.
 */
case class FixedLengthGeneratorFunction(length: Int) extends LengthGeneratorFunction {
  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): LengthGeneratorFunction =
    this

  override def apply(rowLoc: RowLocation): Int = length
}

/**
 * Generate the data with a variable length. Note this will cause skew due to different
 * possible cardinality for the different lengths.
 */
case class VarLengthGeneratorFunction(minLength: Int, maxLength: Int) extends
  LengthGeneratorFunction {
  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): LengthGeneratorFunction =
    this
  override def apply(rowLoc: RowLocation): Int = {
    Random.nextInt(maxLength - minLength + 1) + minLength
  }
}

/**
 * Generate nulls with a given probability.
 * @param prob 0.0 to 1.0 for how often nulls should appear in the output.
 */
case class NullProbabilityGenerationFunction(prob: Double,
    gen: GeneratorFunction = null,
    mapping: LocationToSeedMapping = null) extends NullGeneratorFunction {

  override def withWrapped(gen: GeneratorFunction): NullProbabilityGenerationFunction =
    NullProbabilityGenerationFunction(prob, gen, mapping)

  override def withLocationToSeedMapping(
      mapping: LocationToSeedMapping): NullProbabilityGenerationFunction =
    NullProbabilityGenerationFunction(prob, gen, mapping)

  override def apply(rowLoc: RowLocation): Any = {
    val r = DataGen.getRandomFor(rowLoc, mapping)
    if (r.nextDouble() <= prob) {
      null
    } else {
      gen(rowLoc)
    }
  }
}

/**
 * A LocationToSeedMapping that generates seeds with an approximately equal chance for
 * all values.
 */
case class FlatDistribution(colLocSeed: Long = 0L,
    remapRangeFunc: Long => Long = n => n) extends LocationToSeedMapping {

  override def withColumnConf(colConf: ColumnConf): FlatDistribution = {
    val colLocSeed = colConf.columnLoc.hashLoc
    val remapRangeFunc = LocationToSeedMapping.remapRangeFunc(colConf)
    FlatDistribution(colLocSeed, remapRangeFunc)
  }

  override def apply(rowLoc: RowLocation): Long =
    remapRangeFunc(rowLoc.hashLoc(colLocSeed))
}

/**
 * A LocationToSeedMapping that generates seeds with a normal distribution around a
 * given mean, and with a desired standard deviation. Note that the
 * seeds produced are bound to the seed range requested so if the range is too small
 * or outside of the expected mean the results will not have the expected distribution
 * of values.
 * @param mean the desired mean as a double, defaults to 0
 * @param stdDev the desired standard deviation defaults to 10
 */
case class NormalDistribution(mean: Double = 0.0,
    stdDev: Double = 10.0,
    min: Long = Long.MinValue,
    max: Long = Long.MaxValue,
    colLocSeed: Long = 0) extends LocationToSeedMapping {

  override def withColumnConf(colConf: ColumnConf): NormalDistribution = {
    val colLocSeed = colConf.columnLoc.hashLoc
    NormalDistribution(mean, stdDev, colConf.minSeed, colConf.maxSeed, colLocSeed)
  }

  override def apply(rowLoc: RowLocation): Long = {
    val r = DataGen.getRandomFor(rowLoc.hashLoc(colLocSeed))
    val g = r.nextGaussian() // g has a mean of 0 and a stddev of 1.0
    val adjusted = mean + (g * stdDev)
    // If the range of seeds is too small compared to the stddev and mean we will
    // end up with an invalid distribution, but they asked for it.
    math.max(min, math.min(max, math.round(adjusted).toLong))
  }
}

/**
 * An exponential distribution that will target a specific seed. That target seed will be the
 * maximum value in the distribution. values below it will back off exponentially. There will
 * be no values larger than the target. Note that the seeds produced are bound to the seed range
 * requested. If the target is not within that range or if the rate is such that a large number
 * of seeds might appear outside of the range, then the distribution will be off.
 * @param target the maximum seed value to return, and the most common seed value. Defaults to 0
 * @param stdDev the standard deviation of the distribution. This is 1/the rate, but we use
 *               standard deviation for consistency with other mappings. The default is 1.0
 */
case class ExponentialDistribution(
    target: Double = 0.0,
    stdDev: Double = 1.0,
    min: Long = Long.MinValue,
    max: Long = Long.MaxValue,
    colLocSeed: Long = 0) extends LocationToSeedMapping {
  override def withColumnConf(colConf: ColumnConf): ExponentialDistribution = {
    val colLocSeed = colConf.columnLoc.hashLoc
    ExponentialDistribution(target, stdDev, colConf.minSeed, colConf.maxSeed, colLocSeed)
  }

  override def apply(rowLoc: RowLocation): Long = {
    val r = DataGen.getRandomFor(rowLoc.hashLoc(colLocSeed))
    val g = r.nextDouble()
    val logged = math.log(1.0 - g)
    val adjusted = logged * stdDev
    // This adjusted value targets 0.0 right now and will go negative from there.
    math.max(min, math.min(max, math.ceil(adjusted + target).toLong))
  }
}

object MultiDistribution {
  /**
   * Create a LocationToSeedMapping that combines multiple other LocationsToSeed mappings
   * together based on weights.
   * @param weightsNMappings The weights and mappings to use. The weights are relative to each
   *                         other. So if you want two mappings with one applied 5 times more
   *                         often than the other. You can set the weight of the more desirable
   *                         one to 5.0 and the other to 1.0.
   * @return a LocationToSeedMapping that fits the desired distribution.
   */
  def apply(weightsNMappings: Seq[(Double, LocationToSeedMapping)]): LocationToSeedMapping = {
    if (weightsNMappings.length <= 0) {
      throw new IllegalArgumentException("Need at least one mapping")
    }

    if (weightsNMappings.exists(_._1 <= 0.0)) {
      throw new IllegalArgumentException("All weights must be positive")
    }

    if (weightsNMappings.length == 1) {
      weightsNMappings.head._2
    } else {
      val weightSum = weightsNMappings.map(_._1).sum
      val normalizedWeights = weightsNMappings.map(_._1 / weightSum)
      val runningNormalizedWeights = normalizedWeights.tail
          .scanLeft(normalizedWeights.head)(_ + _).toArray
      val mappings = weightsNMappings.map(_._2).toArray
      MultiDistribution(runningNormalizedWeights, mappings, 0)
    }
  }
}

case class MultiDistribution private (normalizedWeights: Array[Double],
    mappings: Array[LocationToSeedMapping],
    colLocSeed: Long) extends LocationToSeedMapping {
  require(normalizedWeights.length == mappings.length,
    s"${normalizedWeights.toList} vs ${mappings.toList}")
  require(normalizedWeights.length > 0)

  override def withColumnConf(colConf: ColumnConf): LocationToSeedMapping = {
    val colLocSeed = colConf.columnLoc.hashLoc
    val newMappings = mappings.map(_.withColumnConf(colConf))
    MultiDistribution(normalizedWeights, newMappings, colLocSeed)
  }

  override def apply(rowLoc: RowLocation): Long = {
    val r = DataGen.getRandomFor(rowLoc.hashLoc(colLocSeed))
    val lookingFor = r.nextDouble()
    var i = 0
    while (i < (normalizedWeights.length - 1) && normalizedWeights(i) < lookingFor) {
      i += 1
    }
    mappings(i)(rowLoc)
  }
}

/**
 * A LocationToSeedMapping that generates a unique seed per row. The order in which the values are
 * generated is some what of a random like order. This should *NOT* be applied to any
 * columns that are under an array because it ues rowNum and assumes that this maps correctly
 * to the number of rows being generated.
 */
case class DistinctDistribution(numTableRows: Long = Long.MaxValue,
    columnLocSeed: Long = 0L,
    minSeed: Long = Long.MinValue,
    maxSeed: Long = Long.MaxValue) extends LocationToSeedMapping {

  override def withColumnConf(colConf: ColumnConf): DistinctDistribution = {
    val numTableRows = colConf.numTableRows
    val maskLen = 64 - java.lang.Long.numberOfLeadingZeros(numTableRows)
    val maxMask = (1 << maskLen) - 1
    val columnLocSeed = colConf.columnLoc.hashLoc & maxMask
    val minSeed = colConf.minSeed
    val maxSeed = colConf.maxSeed
    DistinctDistribution(numTableRows, columnLocSeed, minSeed, maxSeed)
  }

  override def apply(rowLoc: RowLocation): Long = {
    assert(rowLoc.subRows == null,
      "DistinctDistribution cannot be applied to columns under an Array")
    val range = BigInt(maxSeed) - minSeed
    val modded = BigInt(rowLoc.rowNum).mod(range)
    val rowSeed = (modded + minSeed).toLong
    val ret = rowSeed ^ columnLocSeed
    if (ret > numTableRows) {
      rowSeed
    } else {
      ret
    }
  }
}

object DataGen {
  val rLocal = new ThreadLocal[Random] {
    override def initialValue(): Random = new XORShiftRandom()
  }

  /**
   * Get a Random instance that is based off of a given seed. This should not be kept in
   * between calls to apply, and should not be used after calling a child methods apply.
   */
  def getRandomFor(locSeed: Long): Random = {
    val r = rLocal.get()
    r.setSeed(locSeed)
    r
  }

  /**
   * Get a Random instance that is based off of the given location and mapping.
   */
  def getRandomFor(rowLoc: RowLocation, mapping: LocationToSeedMapping): Random =
    getRandomFor(mapping(rowLoc))

  /**
   * Get a long value that is mapped from the given location and mapping.
   */
  def nextLong(rowLoc: RowLocation, mapping: LocationToSeedMapping): Long =
    getRandomFor(rowLoc, mapping).nextLong()

  /**
   * Produce a remapping function that remaps a full range of long values to a new range evenly.
   */
  def remapRangeFunc(typeMinVal: Long, typeMaxVal: Long): Long => Long = {
    if (typeMinVal == Long.MinValue && typeMaxVal == Long.MaxValue) {
      n => n
    } else {
      // We generate numbers between typeMinVal and typeMaxVal + 1, and then take the floor so that
      // all numbers get an even chance.
      val scaleFactor = (BigDecimal(typeMaxVal) + 1 - typeMinVal) /
          (BigDecimal(Long.MaxValue) - Long.MinValue)
      val minBig = BigDecimal(Long.MinValue)
      n => {
        (((n - minBig) * scaleFactor) + typeMinVal).setScale(0, RoundingMode.FLOOR).toLong
      }
    }
  }
}

/**
 * An expression that actually does the data generation based on an input row number.
 */
case class DataGenExpr(child: Expression,
    override val dataType: DataType,
    canHaveNulls: Boolean,
    f: GeneratorFunction) extends DataGenExprBase {

  override def nullable: Boolean = canHaveNulls

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType)

  override def eval(input: InternalRow): Any = {
    val rowLoc = new RowLocation(child.eval(input).asInstanceOf[Long])
    f(rowLoc)
  }
}

/**
 * Base class for generating a column/sub-column. This holds configuration for the column,
 * and handles what is needed to convert it into GeneratorFunction
 */
abstract class DataGen(var conf: ColumnConf,
    defaultValueRange: Option[(Any, Any)],
    var seedMapping: LocationToSeedMapping = FlatDistribution(),
    var nullMapping: LocationToSeedMapping = FlatDistribution(),
    var lengthGen: LengthGeneratorFunction = FixedLengthGeneratorFunction(10)) {
  protected var userProvidedValueGen: Option[GeneratorFunction] = None
  protected var userProvidedNullGen: Option[NullGeneratorFunction] = None
  protected var valueRange: Option[(Any, Any)] = defaultValueRange

  /**
   * Set a value range for this data gen.
   */
  def setValueRange(min: Any, max: Any): DataGen = {
    valueRange = Some((min, max))
    this
  }

  /**
   * Set a custom GeneratorFunction to use for this column.
   */
  def setValueGen(f: GeneratorFunction): DataGen = {
    userProvidedValueGen = Some(f)
    this
  }

  /**
   * Set a NullGeneratorFunction for this column. This will not be used
   * if the column is not nullable.
   */
  def setNullGen(f: NullGeneratorFunction): DataGen = {
    this.userProvidedNullGen = Some(f)
    this
  }

  /**
   * Set the probability of a null appearing in the output. The probability should be
   * 0.0 to 1.0.
   */
  def setNullProbability(probability: Double): DataGen = {
    this.userProvidedNullGen = Some(NullProbabilityGenerationFunction(probability))
    this
  }

  /**
   * Set a specific location to seed mapping for the value generation.
   */
  def setSeedMapping(seedMapping: LocationToSeedMapping): DataGen = {
    this.seedMapping = seedMapping
    this
  }

  /**
   * Set a specific location to seed mapping for the null generation.
   */
  def setNullMapping(nullMapping: LocationToSeedMapping): DataGen = {
    this.nullMapping = nullMapping
    this
  }

  /**
   * Set a specific LengthGeneratorFunction to use. This will only be used if
   * the datatype needs a length.
   */
  def setLengthGen(lengthGen: LengthGeneratorFunction): DataGen = {
    this.lengthGen = lengthGen
    this
  }

  /**
   * Set the length generation to be a fixed length.
   */
  def setLength(len: Int): DataGen = {
    this.lengthGen = FixedLengthGeneratorFunction(len)
    this
  }

  def setLength(minLen: Int, maxLen: Int) = {
    this.lengthGen = VarLengthGeneratorFunction(minLen, maxLen)
    this
  }

  /**
   * Add this column to a specific correlated key group. This should not be
   * called directly by users.
   */
  def setCorrelatedKeyGroup(keyGroup: Long,
      minSeed: Long, maxSeed: Long,
      seedMapping: LocationToSeedMapping): DataGen = {
    conf = conf.forCorrelatedKeyGroup(keyGroup)
        .forSeedRange(minSeed, maxSeed)
    this.seedMapping = seedMapping
    this
  }

  /**
   * Set a range of seed values that should be returned by the LocationToSeedMapping
   */
  def setSeedRange(min: Long, max: Long): DataGen = {
    conf = conf.forSeedRange(min, max)
    this
  }

  /**
   * Get the default value generator for this specific data gen.
   */
  protected def getValGen: GeneratorFunction

  /**
   * Get the final ready to use GeneratorFunction for the data generator.
   */
  def getGen: GeneratorFunction = {
    val sm = seedMapping.withColumnConf(conf)
    val lg = lengthGen.withLocationToSeedMapping(sm)
    var valGen = userProvidedValueGen.getOrElse(getValGen)
        .withLocationToSeedMapping(sm)
        .withLengthGeneratorFunction(lg)
    valueRange.foreach {
      case (min, max) =>
        valGen = valGen.withValueRange(min, max)
    }
    if (nullable && userProvidedNullGen.isDefined) {
      val nullColConf = conf.forNulls
      val nm = nullMapping.withColumnConf(nullColConf)
      userProvidedNullGen.get
          .withWrapped(valGen)
          .withLocationToSeedMapping(nm)
    } else {
      valGen
    }
  }

  /**
   * Get the data type for this column
   */
  def dataType: DataType

  /**
   * Is this column nullable or not.
   */
  def nullable: Boolean = conf.nullable

  /**
   * Get a child column for a given name, if it has one.
   */
  final def apply(name: String): DataGen = {
    get(name).getOrElse{
      throw new IllegalStateException(s"Could not find a child $name for $this")
    }
  }

  def get(name: String): Option[DataGen] = None
}

/**
 * A special GeneratorFunction that just returns the computed seed. This is helpful for
 * debugging distributions or if you want long values without any abstraction in between.
 */
case class SeedPassThrough(mapping: LocationToSeedMapping = null) extends GeneratorFunction {
  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): GeneratorFunction =
    SeedPassThrough(mapping)

  override def withValueRange(min: Any, max: Any): GeneratorFunction =
    throw new IllegalStateException("Value range is not supported")

  override def apply(rowLoc: RowLocation): Any =
    mapping(rowLoc)
}

/**
 * A special GeneratorFunction that just returns the row number. This can be helpful for
 * visualizing what is happening.
 */
case object RowNumPassThrough extends GeneratorFunction {
  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): GeneratorFunction =
    RowNumPassThrough

  override def withValueRange(min: Any, max: Any): GeneratorFunction =
    throw new IllegalStateException("Value range is not supported")

  override def apply(rowLoc: RowLocation): Any = rowLoc.rowNum
}

/**
 * A Value generator for a single value
 * @param v the value to return
 */
case class SingleValGenFunc(v: Any) extends GeneratorFunction {
  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): SingleValGenFunc = {
    this
  }
  override def apply(rowLoc: RowLocation): Any = v

  override def withValueRange(min: Any, max: Any): GeneratorFunction = {
    if (min != max && min != v) {
      throw new IllegalArgumentException(s"The only value supported for this range is $v")
    }
    this
  }
}

/**
 * Pick a value out of the list of possible values and return that.
 */
case class EnumValGenFunc(values: Array[_ <: Any],
    mapping: LocationToSeedMapping = null) extends GeneratorFunction {
  require(values != null && values.length > 0)

  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): EnumValGenFunc = {
    EnumValGenFunc(values, mapping)
  }

  override def withValueRange(min: Any, max: Any): GeneratorFunction =
    throw new IllegalArgumentException(s"value ranges are not supported for EnumValueGenFunc")

  override def apply(rowLoc: RowLocation): Any = {
    val r = DataGen.getRandomFor(rowLoc, mapping)
    values(r.nextInt(values.length))
  }
}

/**
 * A value generator for booleans.
 */
case class BooleanGenFunc(mapping: LocationToSeedMapping = null) extends GeneratorFunction {
  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): BooleanGenFunc =
    BooleanGenFunc(mapping)

  override def apply(rowLoc: RowLocation): Any =
    DataGen.getRandomFor(rowLoc, mapping).nextBoolean()

  override def withValueRange(min: Any, max: Any): GeneratorFunction = {
    val minb = BooleanGen.asBoolean(min)
    val maxb = BooleanGen.asBoolean(max)
    if (minb == maxb) {
      SingleValGenFunc(minb)
    } else {
      this
    }
  }
}

object BooleanGen {
  def asBoolean(a: Any): Boolean = a match {
    case b: Boolean => b
    case other =>
      throw new IllegalArgumentException(s"a boolean value range only supports " +
          s"boolean values found $other")
  }
}

class BooleanGen(conf: ColumnConf,
    defaultValueRange: Option[(Any, Any)])
    extends DataGen(conf, defaultValueRange) {

  override def dataType: DataType = BooleanType

  override protected def getValGen: GeneratorFunction = BooleanGenFunc()
}

/**
 * A value generator for Bytes
 */
case class ByteGenFunc(mapping: LocationToSeedMapping = null,
    min: Byte = Byte.MinValue,
    max: Byte = Byte.MaxValue) extends GeneratorFunction {

  private lazy val valueRemapping: Long => Byte = ByteGen.remapRangeFunc(min, max)

  override def apply(rowLoc: RowLocation): Any = valueRemapping(DataGen.nextLong(rowLoc, mapping))

  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): ByteGenFunc =
    ByteGenFunc(mapping, min, max)

  override def withValueRange(min: Any, max: Any): GeneratorFunction = {
    var bmin = ByteGen.asByte(min)
    var bmax = ByteGen.asByte(max)
    if (bmin > bmax) {
      val tmp = bmin
      bmin = bmax
      bmax = tmp
    }

    if (bmin == bmax) {
      SingleValGenFunc(bmin)
    } else {
      ByteGenFunc(mapping, bmin, bmax)
    }
  }
}

object ByteGen {
  def asByte(a: Any): Byte = a match {
    case n: Byte => n
    case n: Short if n == n.toByte => n.toByte
    case n: Int if n == n.toByte => n.toByte
    case n: Long if n == n.toByte => n.toByte
    case other =>
      throw new IllegalArgumentException(s"a byte value range only supports " +
          s"byte values found $other")
  }

  def remapRangeFunc(min: Byte, max: Byte): Long => Byte = {
    val wrapped = DataGen.remapRangeFunc(min, max)
    n => wrapped(n).toByte
  }
}

class ByteGen(conf: ColumnConf,
    defaultValueRange: Option[(Any, Any)])
    extends DataGen(conf, defaultValueRange) {
  override def getValGen: GeneratorFunction = ByteGenFunc()
  override def dataType: DataType = ByteType
}

/**
 * A value generator for Shorts
 */
case class ShortGenFunc(mapping: LocationToSeedMapping = null,
    min: Short = Short.MinValue,
    max: Short = Short.MaxValue) extends GeneratorFunction {

  private lazy val valueRemapping: Long => Short =
    ShortGen.remapRangeFunc(min, max)

  override def apply(rowLoc: RowLocation): Any = valueRemapping(DataGen.nextLong(rowLoc, mapping))

  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): ShortGenFunc =
    ShortGenFunc(mapping, min, max)

  override def withValueRange(min: Any, max: Any): GeneratorFunction = {
    var smin = ShortGen.asShort(min)
    var smax = ShortGen.asShort(max)
    if (smin > smax) {
      val tmp = smin
      smin = smax
      smax = tmp
    }

    if (smin == smax) {
      SingleValGenFunc(smin)
    } else {
      ShortGenFunc(mapping, smin, smax)
    }
  }
}

object ShortGen {
  def asShort(a: Any): Short = a match {
    case n: Byte => n.toShort
    case n: Short => n
    case n: Int if n == n.toShort => n.toShort
    case n: Long if n == n.toShort => n.toShort
    case other =>
      throw new IllegalArgumentException(s"a short value range only supports " +
          s"short values found $other")
  }

  def remapRangeFunc(min: Short, max: Short): Long => Short = {
    val wrapped = DataGen.remapRangeFunc(min, max)
    n => wrapped(n).toShort
  }
}

class ShortGen(conf: ColumnConf,
    defaultValueRange: Option[(Any, Any)])
    extends DataGen(conf, defaultValueRange) {
  override def getValGen: GeneratorFunction = ShortGenFunc()

  override def dataType: DataType = ShortType
}

/**
 * A value generator for Ints
 */
case class IntGenFunc(mapping: LocationToSeedMapping = null,
    min: Int = Int.MinValue,
    max: Int = Int.MaxValue) extends GeneratorFunction {

  private lazy val valueRemapping: Long => Int = IntGen.remapRangeFunc(min, max)

  override def apply(rowLoc: RowLocation): Any = valueRemapping(DataGen.nextLong(rowLoc, mapping))

  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): IntGenFunc =
    IntGenFunc(mapping, min, max)

  override def withValueRange(min: Any, max: Any): GeneratorFunction = {
    var imin = IntGen.asInt(min)
    var imax = IntGen.asInt(max)
    if (imin > imax) {
      val tmp = imin
      imin = imax
      imax = tmp
    }

    if (imin == imax) {
      SingleValGenFunc(imin)
    } else {
      IntGenFunc(mapping, imin, imax)
    }
  }
}

object IntGen {
  def asInt(a: Any): Int = a match {
    case n: Byte => n.toInt
    case n: Short => n.toInt
    case n: Int => n
    case n: Long if n == n.toInt => n.toInt
    case other =>
      throw new IllegalArgumentException(s"a int value range only supports " +
          s"int values found $other")
  }

  def remapRangeFunc(min: Int, max: Int): Long => Int = {
    val wrapped = DataGen.remapRangeFunc(min, max)
    n => wrapped(n).toInt
  }
}

class IntGen(conf: ColumnConf,
    defaultValueRange: Option[(Any, Any)])
    extends DataGen(conf, defaultValueRange) {
  override def getValGen: GeneratorFunction = IntGenFunc()

  override def dataType: DataType = IntegerType
}

/**
 * A value generator for Longs
 */
case class LongGenFunc(mapping: LocationToSeedMapping = null,
    min: Long = Long.MinValue,
    max: Long = Long.MaxValue) extends GeneratorFunction {

  protected lazy val valueRemapping: Long => Long = LongGen.remapRangeFunc(min, max)

  override def apply(rowLoc: RowLocation): Any = valueRemapping(DataGen.nextLong(rowLoc, mapping))

  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): LongGenFunc =
    LongGenFunc(mapping, min, max)

  override def withValueRange(min: Any, max: Any): GeneratorFunction = {
    var lmin = LongGen.asLong(min)
    var lmax = LongGen.asLong(max)
    if (lmin > lmax) {
      val tmp = lmin
      lmin = lmax
      lmax = tmp
    }

    if (lmin == lmax) {
      SingleValGenFunc(lmin)
    } else {
      LongGenFunc(mapping, lmin, lmax)
    }
  }
}

object LongGen {
  def asLong(a: Any): Long = a match {
    case n: Byte => n.toLong
    case n: Short => n.toLong
    case n: Int => n.toLong
    case n: Long => n
    case other =>
      throw new IllegalArgumentException(s"a long value range only supports " +
          s"long values found $other")
  }

  def remapRangeFunc(min: Long, max: Long): Long => Long =
    DataGen.remapRangeFunc(min, max)
}

class LongGen(conf: ColumnConf,
    defaultValueRange: Option[(Any, Any)])
    extends DataGen(conf, defaultValueRange) {
  override def getValGen: GeneratorFunction = LongGenFunc()

  override def dataType: DataType = LongType
}

case class Decimal32GenFunc(
    precision: Int,
    scale: Int,
    unscaledMin: Int,
    unscaledMax: Int,
    mapping: LocationToSeedMapping = null) extends GeneratorFunction {

  private lazy val valueRemapping: Long => Int = IntGen.remapRangeFunc(unscaledMin, unscaledMax)

  override def apply(rowLoc: RowLocation): Any =
    Decimal(valueRemapping(DataGen.nextLong(rowLoc, mapping)), precision, scale)

  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): GeneratorFunction =
    Decimal32GenFunc(precision, scale, unscaledMin, unscaledMax, mapping)

  override def withValueRange(min: Any, max: Any): GeneratorFunction = {
    var imin = DecimalGen.asUnscaledInt(min, precision, scale)
    var imax = DecimalGen.asUnscaledInt(max, precision, scale)
    if (imin > imax) {
      val tmp = imin
      imin = imax
      imax = tmp
    }

    if (imin == imax) {
      SingleValGenFunc(Decimal(imin, precision, scale))
    } else {
      Decimal32GenFunc(precision, scale, imin, imax, mapping)
    }
  }
}

case class Decimal64GenFunc(
    precision: Int,
    scale: Int,
    unscaledMin: Long,
    unscaledMax: Long,
    mapping: LocationToSeedMapping = null) extends GeneratorFunction {

  private lazy val valueRemapping: Long => Long = LongGen.remapRangeFunc(unscaledMin, unscaledMax)

  override def apply(rowLoc: RowLocation): Any =
    Decimal(valueRemapping(DataGen.nextLong(rowLoc, mapping)), precision, scale)

  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): GeneratorFunction =
    Decimal64GenFunc(precision, scale, unscaledMin, unscaledMax, mapping)

  override def withValueRange(min: Any, max: Any): GeneratorFunction = {
    var lmin = DecimalGen.asUnscaledLong(min, precision, scale)
    var lmax = DecimalGen.asUnscaledLong(max, precision, scale)
    if (lmin > lmax) {
      val tmp = lmin
      lmin = lmax
      lmax = tmp
    }

    if (lmin == lmax) {
      SingleValGenFunc(Decimal(lmin, precision, scale))
    } else {
      Decimal64GenFunc(precision, scale, lmin, lmax, mapping)
    }
  }
}

case class DecimalGenFunc(
    precision: Int,
    scale: Int,
    unscaledMin: BigInt,
    unscaledMax: BigInt,
    mapping: LocationToSeedMapping = null) extends GeneratorFunction {

  private lazy val valueRemapping: Long => BigInt =
    DecimalGen.remapRangeFunc(unscaledMin, unscaledMax)

  override def apply(rowLoc: RowLocation): Any = {
    val bi = valueRemapping(DataGen.nextLong(rowLoc, mapping)).bigInteger
    Decimal(new JavaBigDecimal(bi, scale))
  }

  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): GeneratorFunction =
    DecimalGenFunc(precision, scale, unscaledMin, unscaledMax, mapping)

  override def withValueRange(min: Any, max: Any): GeneratorFunction = {
    var lmin = DecimalGen.asUnscaled(min, precision, scale)
    var lmax = DecimalGen.asUnscaled(max, precision, scale)
    if (lmin > lmax) {
      val tmp = lmin
      lmin = lmax
      lmax = tmp
    }

    if (lmin == lmax) {
      SingleValGenFunc(Decimal(new JavaBigDecimal(lmin.bigInteger, scale)))
    } else {
      DecimalGenFunc(precision, scale, lmin, lmax, mapping)
    }
  }
}

object DecimalGen {
  def toUnscaledInt(d: JavaBigDecimal, precision: Int, scale: Int): Int = {
    val tmp = d.setScale(scale)
    require(tmp.precision() <= precision, "The value is not in the supported precision range")
    tmp.unscaledValue().intValueExact()
  }

  def asUnscaledInt(a: Any, precision: Int, scale: Int): Int = a match {
    case d: BigDecimal =>
      val tmp = d.setScale(scale)
      toUnscaledInt(tmp.bigDecimal, precision, scale)
    case d: JavaBigDecimal =>
      toUnscaledInt(d, precision, scale)
    case d: Decimal =>
      toUnscaledInt(d.toJavaBigDecimal, precision, scale)
    case n: Byte =>
      toUnscaledInt(new JavaBigDecimal(n), precision, scale)
    case n: Short =>
      toUnscaledInt(new JavaBigDecimal(n), precision, scale)
    case n: Int =>
      toUnscaledInt(new JavaBigDecimal(n), precision, scale)
    case n: Long =>
      toUnscaledInt(new JavaBigDecimal(n), precision, scale)
    case n: Float =>
      toUnscaledInt(new JavaBigDecimal(n), precision, scale)
    case n: Double =>
      toUnscaledInt(new JavaBigDecimal(n), precision, scale)
    case other =>
      throw new IllegalArgumentException(s"a decimal 32 value range only supports " +
          s"decimal values found $other")
  }

  def toUnscaledLong(d: JavaBigDecimal, precision: Int, scale: Int): Long = {
    val tmp = d.setScale(scale)
    require(tmp.precision() <= precision, "The value is not in the supported precision range")
    tmp.unscaledValue().longValueExact()
  }

  def asUnscaledLong(a: Any, precision: Int, scale: Int): Long = a match {
    case d: BigDecimal =>
      val tmp = d.setScale(scale)
      toUnscaledLong(tmp.bigDecimal, precision, scale)
    case d: JavaBigDecimal =>
      toUnscaledLong(d, precision, scale)
    case d: Decimal =>
      toUnscaledLong(d.toJavaBigDecimal, precision, scale)
    case n: Byte =>
      toUnscaledLong(new JavaBigDecimal(n), precision, scale)
    case n: Short =>
      toUnscaledLong(new JavaBigDecimal(n), precision, scale)
    case n: Int =>
      toUnscaledLong(new JavaBigDecimal(n), precision, scale)
    case n: Long =>
      toUnscaledLong(new JavaBigDecimal(n), precision, scale)
    case n: Float =>
      toUnscaledLong(new JavaBigDecimal(n), precision, scale)
    case n: Double =>
      toUnscaledLong(new JavaBigDecimal(n), precision, scale)
    case other =>
      throw new IllegalArgumentException(s"a decimal 64 value range only supports " +
          s"decimal values found $other")
  }

  def toUnscaled(d: JavaBigDecimal, precision: Int, scale: Int): BigInt = {
    val tmp = d.setScale(scale)
    require(tmp.precision() <= precision, "The value is not in the supported precision range")
    tmp.unscaledValue()
  }

  def asUnscaled(a: Any, precision: Int, scale: Int): BigInt = a match {
    case d: BigDecimal =>
      val tmp = d.setScale(scale)
      toUnscaled(tmp.bigDecimal, precision, scale)
    case d: JavaBigDecimal =>
      toUnscaled(d, precision, scale)
    case d: Decimal =>
      toUnscaled(d.toJavaBigDecimal, precision, scale)
    case n: Byte =>
      toUnscaled(new JavaBigDecimal(n), precision, scale)
    case n: Short =>
      toUnscaled(new JavaBigDecimal(n), precision, scale)
    case n: Int =>
      toUnscaled(new JavaBigDecimal(n), precision, scale)
    case n: Long =>
      toUnscaled(new JavaBigDecimal(n), precision, scale)
    case n: Float =>
      toUnscaled(new JavaBigDecimal(n), precision, scale)
    case n: Double =>
      toUnscaled(new JavaBigDecimal(n), precision, scale)
    case other =>
      throw new IllegalArgumentException(s"a decimal value range only supports " +
          s"decimal values found $other")
  }

  def genMaxUnscaledInt(precision: Int): Int = {
    require(precision >= 0 && precision <= Decimal.MAX_INT_DIGITS)
    genMaxUnscaled(precision).toInt
  }

  def genMaxUnscaledLong(precision: Int): Long = {
    require(precision > Decimal.MAX_INT_DIGITS && precision <= Decimal.MAX_LONG_DIGITS)
    genMaxUnscaled(precision).toLong
  }

  def genMaxUnscaled(precision: Int): BigInt =
    BigInt(10).pow(precision) - 1

  def remapRangeFunc(minVal: BigInt, maxVal: BigInt): Long => BigInt = {
    // We generate numbers between minVal and maxVal + 1, and then take the floor so that
    // all numbers get an even chance.
    val minValBD = BigDecimal(minVal)
    val scaleFactor = (BigDecimal(maxVal) + 1 - minValBD) /
        (BigDecimal(Long.MaxValue) - Long.MinValue)
    val minLong = BigDecimal(Long.MinValue)
    n => {
      (((n - minLong) * scaleFactor) + minValBD).setScale(0, RoundingMode.FLOOR).toBigInt
    }
  }
}

class DecimalGen(dt: DecimalType,
    conf: ColumnConf,
    defaultValueRange: Option[(Any, Any)])
    extends DataGen(conf, defaultValueRange) {

  override def dataType: DataType = dt

  override protected def getValGen: GeneratorFunction =
    if (dt.precision <= Decimal.MAX_INT_DIGITS) {
      val max = DecimalGen.genMaxUnscaledInt(dt.precision)
      Decimal32GenFunc(dt.precision, dt.scale, max, -max)
    } else if (dt.precision <= Decimal.MAX_LONG_DIGITS) {
      val max = DecimalGen.genMaxUnscaledLong(dt.precision)
      Decimal64GenFunc(dt.precision, dt.scale, max, -max)
    } else {
      val max = DecimalGen.genMaxUnscaled(dt.precision)
      DecimalGenFunc(dt.precision, dt.scale, -max, max)
    }
}

/**
 * A value generator for Timestamps
 * @param min min value. If you want to overwrite it, call DBGen.setDefaultValueRange with
 *            parameter as like BigDataGenConsts.minTimestampForOrc
 * @param max max value. If you want to overwrite it, call DBGen.setDefaultValueRange with
 *            parameter as like BigDataGenConsts.maxTimestampForOrc
 */
case class TimestampGenFunc(mapping: LocationToSeedMapping = null,
    min: Long = Long.MinValue,
    max: Long = Long.MaxValue) extends GeneratorFunction {

  private lazy val valueRemapping: Long => Long = LongGen.remapRangeFunc(min, max)

  override def apply(rowLoc: RowLocation): Any = valueRemapping(DataGen.nextLong(rowLoc, mapping))

  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): TimestampGenFunc =
    TimestampGenFunc(mapping, min, max)

  override def withValueRange(min: Any, max: Any): GeneratorFunction = {
    var lmin = TimestampGen.asLong(min)
    var lmax = TimestampGen.asLong(max)
    if (lmin > lmax) {
      val tmp = lmin
      lmin = lmax
      lmax = tmp
    }

    if (lmin == lmax) {
      SingleValGenFunc(lmin)
    } else {
      TimestampGenFunc(mapping, lmin, lmax)
    }
  }
}

object TimestampGen {
  def asLong(a: Any): Long = a match {
    case n: Byte => n.toLong
    case n: Short => n.toLong
    case n: Int => n.toLong
    case n: Long => n
    case i: Instant => DateTimeUtils.instantToMicros(i)
    case t: Timestamp => DateTimeUtils.fromJavaTimestamp(t)
    case other =>
      throw new IllegalArgumentException(s"a timestamp value range only supports " +
          s"timestamp or integral values found $other")
  }
}

class TimestampGen(conf: ColumnConf,
    defaultValueRange: Option[(Any, Any)])
    extends DataGen(conf, defaultValueRange) {
  override protected def getValGen: GeneratorFunction = TimestampGenFunc()

  override def dataType: DataType = TimestampType
}

object BigDataGenConsts {
  private val epoch = LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0)
  private val minDate = LocalDateTime.of(1, 1, 1, 0, 0, 0, 0)
  private val maxDate = LocalDateTime.of(9999, 12, 31, 0, 0, 0, 0)
  // same as data_gen.py
  private val minTime = LocalDateTime.of(1, 1, 3, 0, 0, 0, 0)
  // Spark stores timestamps with microsecond precision, no need to specify nanosecond
  private val maxTime = LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999999000)

  // minDateInt = -719162, diff days of(1970-01-01, 0001-01-01)
  val minDateIntForOrc: Int = Duration.between(epoch, minDate).toDays.toInt
  // maxDateInt = 2932896, diff days of(1970-01-01, 9999-12-31)
  val maxDateIntForOrc: Int = Duration.between(epoch, maxDate).toDays.toInt

  // diff microseconds of(1970-01-01, 0001-01-01)
  val minTimestampForOrc = Duration.between(epoch, minTime).toMillis * 1000L
  // diff microseconds of(1970-01-01, 9999-12-31)
  val maxTimestampForOrc = Duration.between(epoch, maxTime).toMillis * 1000L
}

/**
 * A value generator for Dates
 * @param min min value. If you want to overwrite it, call DBGen.setDefaultValueRange with
 *            parameter as like BigDataGenConsts.minDateIntForOrc
 * @param max max value. If you want to overwrite it, call DBGen.setDefaultValueRange with
 *            parameter as like BigDataGenConsts.maxDateIntForOrc
 */
case class DateGenFunc(mapping: LocationToSeedMapping = null,
    min: Int = Int.MinValue,
    max: Int = Int.MaxValue) extends GeneratorFunction {

  private lazy val valueRemapping: Long => Int = IntGen.remapRangeFunc(min, max)

  override def apply(rowLoc: RowLocation): Any = valueRemapping(DataGen.nextLong(rowLoc, mapping))

  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): DateGenFunc =
    DateGenFunc(mapping, min, max)

  override def withValueRange(min: Any, max: Any): GeneratorFunction = {
    var imin = DateGen.asInt(min)
    var imax = DateGen.asInt(max)
    if (imin > imax) {
      val tmp = imin
      imin = imax
      imax = tmp
    }

    if (imin == imax) {
      SingleValGenFunc(imin)
    } else {
      DateGenFunc(mapping, imin, imax)
    }
  }
}

object DateGen {
  def asInt(a: Any): Int = a match {
    case n: Byte => n.toInt
    case n: Short => n.toInt
    case n: Int => n
    case n: Long if n <= Int.MaxValue && n >= Int.MinValue => n.toInt
    case ld: LocalDate => ld.toEpochDay.toInt
    case d: Date => DateTimeUtils.fromJavaDate(d)
    case other =>
      throw new IllegalArgumentException(s"a date value range only supports " +
          s"date or int values found $other")
  }
}

class DateGen(conf: ColumnConf,
    defaultValueRange: Option[(Any, Any)])
    extends DataGen(conf, defaultValueRange) {
  override protected def getValGen: GeneratorFunction = DateGenFunc()

  override def dataType: DataType = DateType
}

/**
 * A value generator for Doubles
 */
case class DoubleGenFunc(mapping: LocationToSeedMapping = null) extends GeneratorFunction {
  override def apply(rowLoc: RowLocation): Any =
    java.lang.Double.longBitsToDouble(DataGen.nextLong(rowLoc, mapping))

  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): DoubleGenFunc =
    DoubleGenFunc(mapping)

  override def withValueRange(min: Any, max: Any): GeneratorFunction =
    throw new IllegalStateException("value ranges are not supported for Double yet")
}

class DoubleGen(conf: ColumnConf, defaultValueRange: Option[(Any, Any)])
    extends DataGen(conf, defaultValueRange) {

  override def dataType: DataType = DoubleType

  override protected def getValGen: GeneratorFunction = DoubleGenFunc()
}

/**
 * A value generator for Floats
 */
case class FloatGenFunc(mapping: LocationToSeedMapping = null) extends GeneratorFunction {
  override def apply(rowLoc: RowLocation): Any =
    java.lang.Float.intBitsToFloat(DataGen.getRandomFor(mapping(rowLoc)).nextInt())

  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): FloatGenFunc =
    FloatGenFunc(mapping)

  override def withValueRange(min: Any, max: Any): GeneratorFunction =
    throw new IllegalStateException("value ranges are not supported for Float yet")
}

class FloatGen(conf: ColumnConf, defaultValueRange: Option[(Any, Any)])
    extends DataGen(conf, defaultValueRange) {

  override def dataType: DataType = FloatType

  override protected def getValGen: GeneratorFunction = FloatGenFunc()
}

trait JSONType {
  def appendRandomValue(sb: StringBuilder,
      index: Int,
      maxStringLength: Int,
      maxArrayLength: Int,
      maxObjectLength: Int,
      depth: Int,
      maxDepth: Int,
      r: Random): Unit
}

object JSONType {
  def selectType(depth: Int,
      maxDepth: Int,
      r: Random): JSONType = {
    val toSelectFrom = if (depth < maxDepth) {
      Seq(QuotedJSONString, JSONLong, JSONDouble, JSONArray, JSONObject)
    } else {
      Seq(QuotedJSONString, JSONLong, JSONDouble)
    }
    val index = r.nextInt(toSelectFrom.length)
    toSelectFrom(index)
  }
}

object QuotedJSONString extends JSONType {
  override def appendRandomValue(sb: StringBuilder,
      index: Int,
      maxStringLength: Int,
      maxArrayLength: Int,
      maxObjectLength: Int,
      depth: Int,
      maxDepth: Int,
      r: Random): Unit = {
    val strValue = r.nextString(r.nextInt(maxStringLength + 1))
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\b", "\\b")
        .replace("\f", "\\f")
    sb.append('"')
    sb.append(strValue)
    sb.append('"')
  }
}

object JSONLong extends JSONType {
  override def appendRandomValue(sb: StringBuilder,
      index: Int,
      maxStringLength: Int,
      maxArrayLength: Int,
      maxObjectLength: Int,
      depth: Int,
      maxDepth: Int,
      r: Random): Unit = {
    sb.append(r.nextLong())
  }
}

object JSONDouble extends JSONType {
  override def appendRandomValue(sb: StringBuilder,
      index: Int,
      maxStringLength: Int,
      maxArrayLength: Int,
      maxObjectLength: Int,
      depth: Int,
      maxDepth: Int,
      r: Random): Unit = {
    sb.append(r.nextDouble() * 4096.0)
  }
}

object JSONArray extends JSONType {
  override def appendRandomValue(sb: StringBuilder,
      index: Int,
      maxStringLength: Int,
      maxArrayLength: Int,
      maxObjectLength: Int,
      depth: Int,
      maxDepth: Int,
      r: Random): Unit = {
    val childType = JSONType.selectType(depth, maxDepth, r)
    val length = r.nextInt(maxArrayLength + 1)
    sb.append("[")
    (0 until length).foreach { i =>
      if (i > 0) {
        sb.append(",")
      }
      childType.appendRandomValue(sb, i, maxStringLength, maxArrayLength, maxObjectLength,
        depth + 1, maxDepth, r)
    }
    sb.append("]")
  }
}

object JSONObject extends JSONType {
  override def appendRandomValue(sb: StringBuilder,
      index: Int,
      maxStringLength: Int,
      maxArrayLength: Int,
      maxObjectLength: Int,
      depth: Int,
      maxDepth: Int,
      r: Random): Unit = {
    val length = r.nextInt(maxObjectLength) + 1
    sb.append("{")
    (0 until length).foreach { i =>
      if (i > 0) {
        sb.append(",")
      }
      sb.append("\"key_")
      sb.append(i)
      sb.append("_")
      sb.append(depth )
      sb.append("\":")
      val childType = JSONType.selectType(depth, maxDepth, r)
      childType.appendRandomValue(sb, i, maxStringLength, maxArrayLength, maxObjectLength,
        depth + 1, maxDepth, r)
    }
    sb.append("}")
  }
}

case class JSONGenFunc(
    maxStringLength: Int,
    maxArrayLength: Int,
    maxObjectLength: Int,
    maxDepth: Int,
    lengthGen: LengthGeneratorFunction = null,
    mapping: LocationToSeedMapping = null) extends GeneratorFunction {

  override def apply(rowLoc: RowLocation): Any = {
    val r = DataGen.getRandomFor(rowLoc, mapping)
    val sb = new StringBuilder()
    JSONObject.appendRandomValue(sb, 0, maxStringLength, maxArrayLength, maxObjectLength,
      0, maxDepth, r)
    // For now I am going to have some hard coded keys
    UTF8String.fromString(sb.toString())
  }

  override def withLengthGeneratorFunction(lengthGen: LengthGeneratorFunction): GeneratorFunction =
    JSONGenFunc(maxStringLength, maxArrayLength, maxObjectLength, maxDepth, lengthGen, mapping)

  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): GeneratorFunction =
    JSONGenFunc(maxStringLength, maxArrayLength, maxObjectLength, maxDepth, lengthGen, mapping)

  override def withValueRange(min: Any, max: Any): GeneratorFunction =
    throw new IllegalArgumentException("value ranges are not supported for strings")
}

case class ASCIIGenFunc(
    lengthGen: LengthGeneratorFunction = null,
    mapping: LocationToSeedMapping = null) extends GeneratorFunction {

  override def apply(rowLoc: RowLocation): Any = {
    val len = lengthGen(rowLoc)
    val r = DataGen.getRandomFor(rowLoc, mapping)
    val buffer = new Array[Byte](len)
    var at = 0
    while (at < len) {
      // Value range is 32 (Space) to 126 (~)
      buffer(at) = (r.nextInt(126 - 31) + 32).toByte
      at += 1
    }
    UTF8String.fromBytes(buffer, 0, len)
  }

  override def withLengthGeneratorFunction(lengthGen: LengthGeneratorFunction): GeneratorFunction =
    ASCIIGenFunc(lengthGen, mapping)

  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): GeneratorFunction =
    ASCIIGenFunc(lengthGen, mapping)

  override def withValueRange(min: Any, max: Any): GeneratorFunction =
    throw new IllegalArgumentException("value ranges are not supported for strings")
}

class StringGen(conf: ColumnConf, defaultValueRange: Option[(Any, Any)])
    extends DataGen(conf, defaultValueRange) {

  override def dataType: DataType = StringType

  override protected def getValGen: GeneratorFunction = ASCIIGenFunc()
}

case class StructGenFunc(childGens: Array[GeneratorFunction]) extends GeneratorFunction {
  override def apply(rowLoc: RowLocation): Any = {
    // The row location does not change for a struct
    val data = childGens.map(_.apply(rowLoc))
    InternalRow(data: _*)
  }

  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): GeneratorFunction =
    this

  override def withValueRange(min: Any, max: Any): GeneratorFunction =
    throw new IllegalArgumentException("value ranges are not supported by structs")
}

class StructGen(val children: Seq[(String, DataGen)],
    conf: ColumnConf,
    defaultValueRange: Option[(Any, Any)])
    extends DataGen(conf, defaultValueRange) {

  private lazy val dt = {
    val childrenFields = children.map {
      case (name, gen) =>
        StructField(name, gen.dataType)
    }
    StructType(childrenFields)
  }
  override def dataType: DataType = dt

  override def setCorrelatedKeyGroup(keyGroup: Long,
      minSeed: Long, maxSeed: Long,
      seedMapping: LocationToSeedMapping): DataGen = {
    super.setCorrelatedKeyGroup(keyGroup, minSeed, maxSeed, seedMapping)
    children.foreach {
      case (_, gen) =>
        gen.setCorrelatedKeyGroup(keyGroup, minSeed, maxSeed, seedMapping)
    }
    this
  }

  override def get(name: String): Option[DataGen] =
    children.collectFirst {
      case (childName, dataGen) if childName.equalsIgnoreCase(name) => dataGen
    }

  override protected def getValGen: GeneratorFunction = {
    val childGens = children.map(c => c._2.getGen).toArray
    StructGenFunc(childGens)
  }
}

case class ArrayGenFunc(
    child: GeneratorFunction,
    lengthGen: LengthGeneratorFunction = null,
    mapping: LocationToSeedMapping = null) extends GeneratorFunction {

  override def apply(rowLoc: RowLocation): Any = {
    val len = lengthGen(rowLoc)
    val data = new Array[Any](len)
    val childRowLoc = rowLoc.withNewChild()
    var i = 0
    while (i < len) {
      childRowLoc.setLastChildIndex(i)
      data(i) = child(childRowLoc)
      i += 1
    }
    ArrayData.toArrayData(data)
  }

  override def withLengthGeneratorFunction(lengthGen: LengthGeneratorFunction): GeneratorFunction =
    ArrayGenFunc(child, lengthGen, mapping)

  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): GeneratorFunction =
    ArrayGenFunc(child, lengthGen, mapping)

  override def withValueRange(min: Any, max: Any): GeneratorFunction =
    throw new IllegalArgumentException("value ranges are not supported for arrays")
}

class ArrayGen(child: DataGen,
    conf: ColumnConf,
    defaultValueRange: Option[(Any, Any)])
    extends DataGen(conf, defaultValueRange) {

  override def setCorrelatedKeyGroup(keyGroup: Long,
      minSeed: Long, maxSeed: Long,
      seedMapping: LocationToSeedMapping): DataGen = {
    super.setCorrelatedKeyGroup(keyGroup, minSeed, maxSeed, seedMapping)
    child.setCorrelatedKeyGroup(keyGroup, minSeed, maxSeed, seedMapping)
    this
  }

  override protected def getValGen: GeneratorFunction = ArrayGenFunc(child.getGen)

  override def dataType: DataType = ArrayType(child.dataType, containsNull = child.nullable)

  override def get(name: String): Option[DataGen] = {
    if ("data".equalsIgnoreCase(name) || "child".equalsIgnoreCase(name)) {
      Some(child)
    } else {
      None
    }
  }
}

case class MapGenFunc(
    key: GeneratorFunction,
    value: GeneratorFunction,
    lengthGen: LengthGeneratorFunction = null,
    mapping: LocationToSeedMapping = null) extends GeneratorFunction {

  override def apply(rowLoc: RowLocation): Any = {
    val len = lengthGen(rowLoc)
    val jMap = new util.HashMap[Any, Any](len * 2)
    val childRowLoc = rowLoc.withNewChild()
    var i = 0
    while (i < len) {
      childRowLoc.setLastChildIndex(i)
      val keyData = key(childRowLoc)
      val valueData = value(childRowLoc)
      jMap.put(keyData, valueData)
      i += 1
    }
    ArrayBasedMapData(jMap, n => n, n => n)
  }

  override def withLengthGeneratorFunction(lengthGen: LengthGeneratorFunction): GeneratorFunction =
    MapGenFunc(key, value, lengthGen, mapping)

  override def withLocationToSeedMapping(mapping: LocationToSeedMapping): GeneratorFunction =
    MapGenFunc(key, value, lengthGen, mapping)

  override def withValueRange(min: Any, max: Any): GeneratorFunction =
    throw new IllegalArgumentException("value ranges are not supported for maps")
}

class MapGen(key: DataGen,
    value: DataGen,
    conf: ColumnConf,
    defaultValueRange: Option[(Any, Any)])
    extends DataGen(conf, defaultValueRange) {
  require(!key.nullable, "Map keys cannot contain nulls")

  override def setCorrelatedKeyGroup(keyGroup: Long,
      minSeed: Long,
      maxSeed: Long,
      seedMapping: LocationToSeedMapping): DataGen = {
    super.setCorrelatedKeyGroup(keyGroup, minSeed, maxSeed, seedMapping)
    key.setCorrelatedKeyGroup(keyGroup, minSeed, maxSeed, seedMapping)
    value.setCorrelatedKeyGroup(keyGroup, minSeed, maxSeed, seedMapping)
    this

  }

  override protected def getValGen: GeneratorFunction = MapGenFunc(key.getGen, value.getGen)

  override def dataType: DataType = MapType(key.dataType, value.dataType, value.nullable)

  override def get(name: String): Option[DataGen] = {
    if ("key".equalsIgnoreCase(name)) {
      Some(key)
    } else if ("value".equalsIgnoreCase(name)) {
      Some(value)
    } else {
      None
    }
  }
}


object ColumnGen {
  private def genInternal(rowNumber: Column,
      dataType: DataType,
      nullable: Boolean,
      gen: GeneratorFunction): Column = {
    Column(DataGenExpr(rowNumber.expr, dataType, nullable, gen))
  }
}

/**
 * Generates a top level column in a data set.
 */
class ColumnGen(val dataGen: DataGen) {
  def setCorrelatedKeyGroup(kg: Long,
      minSeed: Long, maxSeed: Long,
      seedMapping: LocationToSeedMapping): ColumnGen = {
    dataGen.setCorrelatedKeyGroup(kg, minSeed, maxSeed, seedMapping)
    this
  }

  def setSeedRange(min: Long, max: Long): ColumnGen = {
    dataGen.setSeedRange(min, max)
    this
  }

  def setSeedMapping(seedMapping: LocationToSeedMapping): ColumnGen = {
    dataGen.setSeedMapping(seedMapping)
    this
  }

  def setNullSeedMapping(seedMapping: LocationToSeedMapping): ColumnGen = {
    dataGen.setNullMapping(seedMapping)
    this
  }

  def setValueRange(min: Any, max: Any): ColumnGen = {
    dataGen.setValueRange(min, max)
    this
  }

  def setNullProbability(probability: Double): ColumnGen = {
    dataGen.setNullProbability(probability)
    this
  }

  def setNullGen(f: NullGeneratorFunction): ColumnGen = {
    dataGen.setNullGen(f)
    this
  }

  def setValueGen(f: GeneratorFunction): ColumnGen = {
    dataGen.setValueGen(f)
    this
  }

  def setLengthGen(lengthGen: LengthGeneratorFunction): ColumnGen = {
    dataGen.setLengthGen(lengthGen)
    this
  }

  def setLength(len: Int): ColumnGen = {
    dataGen.setLength(len)
    this
  }

  def setLength(minLen: Int, maxLen: Int): ColumnGen = {
    dataGen.setLength(minLen, maxLen)
    this
  }

  final def apply(name: String): DataGen = {
    get(name).getOrElse {
      throw new IllegalArgumentException(s"$name not a child of $this")
    }
  }

  def get(name: String): Option[DataGen] = dataGen.get(name)

  def gen(rowNumber: Column): Column = {
    ColumnGen.genInternal(rowNumber, dataGen.dataType, dataGen.nullable, dataGen.getGen)
  }
}

sealed trait KeyGroupType

/**
 * A key group where all of the columns and sub-columns use the key group id instead of
 * the column number when calculating the seed for the desired value. This makes all of
 * the generator functions to compute the exact same seed and produce correlated results.
 * For this to work the columns that will be joined to be configured with the same generators
 * and the same value ranges (if any). But the distribution of the seeds and the seed range
 * do not have to correspond.
 * @param id the id of the key group, that replaces the column number.
 * @param minSeed the min seed value for this key group
 * @param maxSeed the max seed value for this key group
 */
case class CorrelatedKeyGroup(id: Long, minSeed: Long, maxSeed: Long) extends KeyGroupType

/**
 * A key group where the seed range is calculated based on the number of columns to
 * get approximately the desired number of unique combinations. This will not be exact
 * and does not work with nested types, or if the cardinality of the columns involved
 * is not large enough to support the desired key range. Much of this can be fixed in
 * the future.
 *
 * If you want to get partially overlapping groups you can increase the number of
 * combinations, but again this is approximate so it might not really do exactly what you want.
 * @param startSeed the seed that all combinatorial groups should start with
 * @param combinations the desired number of unique combinations.
 */
case class CombinatorialKeyGroup(startSeed: Long, combinations: Long) extends KeyGroupType

/**
 * Used to generate a table in a database
 */
class TableGen(val columns: Seq[(String, ColumnGen)], numRows: Long) {
  /**
   * A Key group allows you to setup a multi-column key for things like a join where you
   * want the keys to be generated as a group.
   * @param names the names of the columns in the group. A column can only be a part of a
   *              single group.
   * @param mapping the distribution/mapping for this key group
   * @param groupType the type of key group that this is for.
   * @return this for chaining
   */
  def configureKeyGroup(
      names: Seq[String],
      groupType: KeyGroupType,
      mapping: LocationToSeedMapping): TableGen = {
    groupType match {
      case CorrelatedKeyGroup(id, minSeed, maxSeed) =>
        names.foreach { name =>
          val col = apply(name)
          col.setCorrelatedKeyGroup(id, minSeed, maxSeed, mapping)
        }
      case CombinatorialKeyGroup(startSeed, combinations) =>
        var numberOfCombinationsRemaining = combinations
        var numberOfColumnsRemaining = names.length
        names.foreach { name =>
          val choices = math.pow(numberOfCombinationsRemaining.toDouble,
            1.0 / numberOfColumnsRemaining).toLong
          if (choices <= 0) {
            throw new IllegalArgumentException("Could not find a way to split up " +
                s"${names.length} columns to produce $combinations unique combinations of values")
          }
          val column = apply(name)
          column.setSeedRange(startSeed, startSeed + choices - 1)
          numberOfColumnsRemaining -= 1
          numberOfCombinationsRemaining = numberOfCombinationsRemaining / choices
        }
    }
    this
  }

  /**
   * Convert this table into a `DataFrame` that can be
   * written out or used directly. Writing it out to parquet
   * or ORC is probably better because you will not run into
   * issues with the generation happening inline and performance
   * being bad because this is not on the GPU.
   * @param spark the session to use.
   * @param numParts the number of parts to use (if > 0)
   */
  def toDF(spark: SparkSession, numParts: Int = 0): DataFrame = {
    val id = col("id")
    val allGens = columns.map {
      case (name, childGen) =>
        childGen.gen(id).alias(name)
    }

    val range = if (numParts > 0) {
      spark.range(0, numRows, 1, numParts)
    } else {
      spark.range(numRows)
    }
    range.select(allGens: _*)
  }

  /**
   * Get a ColumnGen for a named column.
   * @param name the name of the column to look for
   * @return the corresponding column gen
   */
  def apply(name: String): ColumnGen = {
    get(name).getOrElse {
      throw new IllegalArgumentException(s"$name not found: ${columns.map(_._1).mkString(" ")}")
    }
  }

  def get(name: String): Option[ColumnGen] =
    columns.collectFirst {
      case (childName, colGen) if childName.equalsIgnoreCase(name) => colGen
    }
}

/**
 * Provides a way to map a DataType to a specific DataGen instance.
 */
trait TypeMapping {
  /**
   * Does this TypeMapping support the give data type.
   * @return true if it does, else false
   */
  def canMap(dt: DataType, subTypeMapping: TypeMapping): Boolean

  /**
   * If canMap returned true, then do the mapping
   * @return the DataGen along with the last ColumnConf that was used by a column
   */
  def map(dt: DataType,
      conf: ColumnConf,
      defaultRanges: mutable.HashMap[DataType, (Any, Any)],
      subTypeMapping: TypeMapping): (DataGen, ColumnConf)
}

object DefaultTypeMapping extends TypeMapping {
  override def canMap(dataType: DataType, subTypeMapping: TypeMapping): Boolean = dataType match {
    case BooleanType => true
    case ByteType => true
    case ShortType => true
    case IntegerType => true
    case LongType => true
    case _: DecimalType => true
    case FloatType => true
    case DoubleType => true
    case StringType => true
    case TimestampType => true
    case DateType => true
    case st: StructType =>
      st.forall(child => subTypeMapping.canMap(child.dataType, subTypeMapping))
    case at: ArrayType =>
      subTypeMapping.canMap(at.elementType, subTypeMapping)
    case mt: MapType =>
      subTypeMapping.canMap(mt.keyType, subTypeMapping) &&
          subTypeMapping.canMap(mt.valueType, subTypeMapping)
    case _ => false
  }

  override def map(dataType: DataType,
      conf: ColumnConf,
      defaultRanges: mutable.HashMap[DataType, (Any, Any)],
      subTypeMapping: TypeMapping): (DataGen, ColumnConf) = dataType match {
    case BooleanType =>
      (new BooleanGen(conf, defaultRanges.get(dataType)), conf)
    case ByteType =>
      (new ByteGen(conf, defaultRanges.get(dataType)), conf)
    case ShortType =>
      (new ShortGen(conf, defaultRanges.get(dataType)), conf)
    case IntegerType =>
      (new IntGen(conf, defaultRanges.get(dataType)), conf)
    case LongType =>
      (new LongGen(conf, defaultRanges.get(dataType)), conf)
    case dt: DecimalType =>
      (new DecimalGen(dt, conf, defaultRanges.get(dataType)), conf)
    case FloatType =>
      (new FloatGen(conf, defaultRanges.get(dataType)), conf)
    case DoubleType =>
      (new DoubleGen(conf, defaultRanges.get(dataType)), conf)
    case StringType =>
      (new StringGen(conf, defaultRanges.get(dataType)), conf)
    case TimestampType =>
      (new TimestampGen(conf, defaultRanges.get(dataType)), conf)
    case DateType =>
      (new DateGen(conf, defaultRanges.get(dataType)), conf)
    case st: StructType =>
      var tmpConf = conf
      val fields = st.map { sf =>
        tmpConf = tmpConf.forNextColumn(sf.nullable)
        val genNCol = subTypeMapping.map(sf.dataType, tmpConf, defaultRanges, subTypeMapping)
        tmpConf = genNCol._2
        (sf.name, genNCol._1)
      }
      (new StructGen(fields, conf, defaultRanges.get(dataType)), tmpConf)
    case at: ArrayType =>
      val childConf = conf.forNextColumn(at.containsNull)
      val child = subTypeMapping.map(at.elementType, childConf, defaultRanges, subTypeMapping)
      (new ArrayGen(child._1, conf, defaultRanges.get(dataType)), child._2)
    case mt: MapType =>
      val keyChildConf = conf.forNextColumn(false)
      val keyInfo = subTypeMapping.map(mt.keyType, keyChildConf, defaultRanges, subTypeMapping)
      val valueChildConf = keyInfo._2.forNextColumn(mt.valueContainsNull)
      val valueInfo = subTypeMapping.map(mt.valueType, valueChildConf, defaultRanges,
        subTypeMapping)
      (new MapGen(keyInfo._1, valueInfo._1, conf, defaultRanges.get(mt)), valueInfo._2)
    case other =>
      throw new IllegalArgumentException(s"$other is not a supported type yet")
  }
}

case class OrderedTypeMapping(ordered: Array[TypeMapping]) extends TypeMapping {
  override def canMap(dt: DataType, subTypeMapping: TypeMapping): Boolean = {
    ordered.foreach { mapping =>
      if (mapping.canMap(dt, subTypeMapping)) {
        return true
      }
    }
    false
  }

  override def map(dt: DataType,
      conf: ColumnConf,
      defaultRanges: mutable.HashMap[DataType, (Any, Any)],
      subTypeMapping: TypeMapping): (DataGen, ColumnConf) = {
    ordered.foreach { mapping =>
      if (mapping.canMap(dt, subTypeMapping)) {
        return mapping.map(dt, conf, defaultRanges, subTypeMapping)
      }
    }
    // This should not be reachable
    throw new IllegalStateException(s"$dt is not currently supported")
  }
}

object DBGen {
  def empty: DBGen = new DBGen()
  def apply(): DBGen = new DBGen()

  private def dtToTopLevelGen(
      st: StructType,
      tableId: Int,
      defaultRanges: mutable.HashMap[DataType, (Any, Any)],
      numRows: Long,
      mapping: OrderedTypeMapping): Seq[(String, ColumnGen)] = {
    // a bit of a hack with the column num so that we update it before each time...
    var conf = ColumnConf(ColumnLocation(tableId, -1), true, numRows)
    st.toArray.map { sf =>
      if (!mapping.canMap(sf.dataType, mapping)) {
        throw new IllegalArgumentException(s"$sf is not supported at this time")
      }
      conf = conf.forNextColumn(sf.nullable)
      val tmp = mapping.map(sf.dataType, conf, defaultRanges, mapping)
      conf = tmp._2
      (sf.name, new ColumnGen(tmp._1))
    }
  }
}

/**
 * Set up the schema for different tables and the relationship between various keys/columns
 * in the tables.
 */
class DBGen {
  private var tableId = 0
  private val tables = mutable.HashMap.empty[String, TableGen]
  private val defaultRanges = mutable.HashMap.empty[DataType, (Any, Any)]
  private val mappings = ArrayBuffer[TypeMapping](DefaultTypeMapping)

  /**
   * Set a default value range for all generators of a given type. Note that this only impacts
   * tables that have not been added yet. Some generators don't support value ranges setting
   * this for a type that does not support it will result in an error when creating one.
   * Some generators can be configured to ignore this too, like if you pass in your own
   * function for data generation. In those cases this may be ignored.
   */
  def setDefaultValueRange(dt: DataType, min: Any, max: Any): DBGen = {
    defaultRanges.put(dt, (min, max))
    this
  }

  /**
   * Add a new table with a given input
   * @param name the name of the table (must be unique)
   * @param columns the generators that will be used for this table
   * @return the TableGen that was added
   */
  private def addTable(name: String,
      columns: Seq[(String, ColumnGen)],
      numRows: Long): TableGen = {
    val lowerName = name.toLowerCase
    if (lowerName.contains(".")) {
      // Not sure if there are other forbidden characters, but we can check on that later
      throw new IllegalArgumentException("Name cannot contain '.' character")
    }
    if (tables.contains(lowerName)) {
      throw new IllegalArgumentException("Cannot add duplicate tables (even if case is different)")
    }
    val ret = new TableGen(columns, numRows)
    tables.put(lowerName, ret)
    ret
  }

  /**
   * Add a new table with a given type
   * @param name the name of the table (must be unique)
   * @param st the type for this table.
   * @param numRows the number of rows for this table
   * @return the TableGen that was just added
   */
  def addTable(name: String,
      st: StructType,
      numRows: Long): TableGen = {
    val localTableId = tableId
    tableId += 1
    val mapping = OrderedTypeMapping(mappings.toArray)
    val sg = DBGen.dtToTopLevelGen(st, localTableId, defaultRanges, numRows, mapping)
    addTable(name, sg, numRows)
  }

  /**
   * Add a new table with a type defined by the DDL
   * @param name the name of the table (must be unique)
   * @param ddl the DDL that describes the type for the table.
   * @param numRows the number of rows for this table
   * @return the TableGen that was just created
   */
  def addTable(name: String,
      ddl: String,
      numRows: Long): TableGen = {
    val localTableId = tableId
    tableId += 1
    val mapping = OrderedTypeMapping(mappings.toArray)
    val sg = DBGen.dtToTopLevelGen(
      DataType.fromDDL(ddl).asInstanceOf[StructType], localTableId, defaultRanges, numRows, mapping)
    addTable(name, sg, numRows)
  }

  /**
   * Get a TableGen by name
   * @param name the name of the table to look for
   * @return the corresponding TableGen
   */
  def apply(name: String): TableGen = tables(name.toLowerCase)

  def get(name: String): Option[TableGen] = tables.get(name.toLowerCase)

  /**
   * Names of the tables.
   */
  def tableNames: Seq[String] = tables.keys.toSeq

  /**
   * Get an immutable map out of this.
   */
  def toMap: Map[String, TableGen] = tables.toMap

  /**
   * Convert all of the tables into dataframes
   * @param spark the session to use for the conversion
   * @param numParts the number of parts (tasks) to use. <= 0 uses the same number of tasks
   *                 as the cluster has.
   * @return a Map of the name of the table to the dataframe.
   */
  def toDF(spark: SparkSession, numParts: Int = 0): Map[String, DataFrame] =
    toMap.map {
      case (name, gen) => (name, gen.toDF(spark, numParts))
    }

  /**
   * Take all of the tables and create or replace temp views with them using the given name.
   * The data will be generated inline as the query runs.
   * @param spark the session to use
   * @param numParts the number of parts to use, if > 0
   */
  def createOrReplaceTempViews(spark: SparkSession, numParts: Int = 0): Unit =
    toDF(spark, numParts).foreach {
      case (name, df) => df.createOrReplaceTempView(name)
    }

  /**
   * Write all of the tables out as parquet under path/table_name and overwrite anything that is
   * already there.
   * @param spark the session to use
   * @param path the base path to write the data under
   * @param numParts the number of parts to use, if > 0
   * @param overwrite if true will overwrite existing data
   */
  def writeParquet(spark: SparkSession,
      path: String,
      numParts: Int = 0,
      overwrite: Boolean = false): Unit = {
    toDF(spark, numParts).foreach {
      case (name, df) =>
        val subPath = path + "/" + name
        var writer = df.write
        if (overwrite) {
          writer = writer.mode("overwrite")
        }
        writer.parquet(subPath)
    }
  }

  /**
   * Create or replace temp views for all of the tables assuming that they were already written
   * out as parquet under path/table_name.
   * @param spark the session to use
   * @param path the base path to read the data from
   */
  def createOrReplaceTempViewsFromParquet(spark: SparkSession, path: String): Unit = {
    tables.foreach {
      case (name, _) =>
        val subPath = path + "/" + name
        spark.read.parquet(subPath).createOrReplaceTempView(name)
    }
  }

  /**
   * Write all of the tables out as parquet under path/table_name and then create or replace temp
   * views for each of the tables using the given name.
   *
   * @param spark the session to use
   * @param path the base path to write the data under
   * @param numParts the number of parts to use, if > 0
   * @param overwrite if true will overwrite existing data
   */
  def writeParquetAndReplaceTempViews(spark: SparkSession,
      path: String,
      numParts: Int = 0,
      overwrite: Boolean = false): Unit = {
    writeParquet(spark, path, numParts, overwrite)
    createOrReplaceTempViewsFromParquet(spark, path)
  }

  /**
   * Write all of the tables out as orc under path/table_name and overwrite anything that is
   * already there.
   *
   * @param spark the session to use
   * @param path the base path to write the data under
   * @param numParts the number of parts to use, if > 0
   * @param overwrite if true will overwrite existing data
   */
  def writeOrc(spark: SparkSession,
      path: String,
      numParts: Int = 0,
      overwrite: Boolean = false): Unit = {
    toDF(spark, numParts).foreach {
      case (name, df) =>
        val subPath = path + "/" + name
        var writer = df.write
        if (overwrite) {
          writer = writer.mode("overwrite")
        }
        writer.orc(subPath)
    }
  }

  /**
   * Create or replace temp views for all of the tables assuming that they were already written
   * out as orc under path/table_name.
   * @param spark the session to use
   * @param path  the base path to read the data from
   */
  def createOrReplaceTempViewsFromOrc(spark: SparkSession, path: String): Unit = {
    tables.foreach {
      case (name, _) =>
        val subPath = path + "/" + name
        spark.read.orc(subPath).createOrReplaceTempView(name)
    }
  }

  /**
   * Write all of the tables out as orc under path/table_name and then create or replace temp
   * views for each of the tables using the given name.
   *
   * @param spark the session to use
   * @param path the base path to write the data under
   * @param numParts the number of parts to use, if > 0
   * @param overwrite if true will overwrite existing data
   */
  def writeOrcAndReplaceTempViews(spark: SparkSession,
      path: String,
      numParts: Int = 0,
      overwrite: Boolean = false): Unit = {
    writeOrc(spark, path, numParts, overwrite)
    createOrReplaceTempViewsFromOrc(spark, path)
  }

  /**
   * Add a new user controlled type mapping. This allows
   * the user to totally override the handling for any or all types.
   * @param mapping the new mapping to add with highest priority.
   */
  def addTypeMapping(mapping: TypeMapping): Unit = {
    // Insert this mapping in front of the others
    mappings.insert(0, mapping)
  }
}
