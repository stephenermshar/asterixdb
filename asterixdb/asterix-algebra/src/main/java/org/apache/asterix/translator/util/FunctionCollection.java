/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.translator.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.runtime.aggregates.collections.FirstElementAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.collections.ListifyAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.collections.LocalFirstElementAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarCountAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarMaxAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarMinAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarSqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarSqlCountAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarSqlMaxAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarSqlMinAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarSqlSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableCountAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableGlobalAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableGlobalSqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableIntermediateAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableIntermediateSqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableLocalAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableLocalSqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableLocalSqlSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableLocalSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableSqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableSqlCountAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableSqlSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.AvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.CountAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.GlobalAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.GlobalSqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.IntermediateAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.IntermediateSqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.LocalAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.LocalMaxAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.LocalMinAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.LocalSqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.LocalSqlMaxAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.LocalSqlMinAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.LocalSqlSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.LocalSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.MaxAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.MinAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.SqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.SqlCountAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.SqlMaxAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.SqlMinAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.SqlSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.SumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.stream.EmptyStreamAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.stream.NonEmptyStreamAggregateDescriptor;
import org.apache.asterix.runtime.evaluators.accessors.CircleCenterAccessor;
import org.apache.asterix.runtime.evaluators.accessors.CircleRadiusAccessor;
import org.apache.asterix.runtime.evaluators.accessors.LineRectanglePolygonAccessor;
import org.apache.asterix.runtime.evaluators.accessors.PointXCoordinateAccessor;
import org.apache.asterix.runtime.evaluators.accessors.PointYCoordinateAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalDayAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalHourAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalIntervalEndAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalIntervalEndDateAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalIntervalEndDatetimeAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalIntervalEndTimeAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalIntervalStartAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalIntervalStartDateAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalIntervalStartDatetimeAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalIntervalStartTimeAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalMillisecondAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalMinuteAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalMonthAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalSecondAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalYearAccessor;
import org.apache.asterix.runtime.evaluators.comparisons.EqualsDescriptor;
import org.apache.asterix.runtime.evaluators.comparisons.GreaterThanDescriptor;
import org.apache.asterix.runtime.evaluators.comparisons.GreaterThanOrEqualsDescriptor;
import org.apache.asterix.runtime.evaluators.comparisons.LessThanDescriptor;
import org.apache.asterix.runtime.evaluators.comparisons.LessThanOrEqualsDescriptor;
import org.apache.asterix.runtime.evaluators.comparisons.NotEqualsDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ABinaryBase64StringConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ABinaryHexStringConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ABooleanConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ACircleConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ADateConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ADateTimeConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ADayTimeDurationConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ADoubleConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ADurationConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AFloatConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AInt16ConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AInt32ConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AInt64ConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AInt8ConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AIntervalConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AIntervalStartFromDateConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AIntervalStartFromDateTimeConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AIntervalStartFromTimeConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ALineConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ANullConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.APoint3DConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.APointConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.APolygonConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ARectangleConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AStringConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ATimeConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AUUIDFromStringConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AYearMonthDurationConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ClosedRecordConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.OpenRecordConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.functions.*;
import org.apache.asterix.runtime.evaluators.functions.binary.BinaryConcatDescriptor;
import org.apache.asterix.runtime.evaluators.functions.binary.BinaryLengthDescriptor;
import org.apache.asterix.runtime.evaluators.functions.binary.FindBinaryDescriptor;
import org.apache.asterix.runtime.evaluators.functions.binary.FindBinaryFromDescriptor;
import org.apache.asterix.runtime.evaluators.functions.binary.ParseBinaryDescriptor;
import org.apache.asterix.runtime.evaluators.functions.binary.PrintBinaryDescriptor;
import org.apache.asterix.runtime.evaluators.functions.binary.SubBinaryFromDescriptor;
import org.apache.asterix.runtime.evaluators.functions.binary.SubBinaryFromToDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.FieldAccessByIndexDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.FieldAccessByNameDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.FieldAccessNestedDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.GetRecordFieldValueDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.GetRecordFieldsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.RecordAddFieldsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.RecordMergeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.RecordPairsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.RecordRemoveFieldsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.AdjustDateTimeForTimeZoneDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.AdjustTimeForTimeZoneDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.CalendarDuartionFromDateDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.CalendarDurationFromDateTimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.CurrentDateDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.CurrentDateTimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.CurrentTimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DateFromDatetimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DateFromUnixTimeInDaysDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DatetimeFromDateAndTimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DatetimeFromUnixTimeInMsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DatetimeFromUnixTimeInSecsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DayOfWeekDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DayTimeDurationGreaterThanComparatorDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DayTimeDurationLessThanComparatorDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DurationEqualDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DurationFromIntervalDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DurationFromMillisecondsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DurationFromMonthsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.GetDayTimeDurationDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.GetOverlappingIntervalDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.GetYearMonthDurationDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalAfterDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalBeforeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalBinDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalCoveredByDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalCoversDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalEndedByDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalEndsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalMeetsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalMetByDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalOverlappedByDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalOverlappingDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalOverlapsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalPartitionJoinEndDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalPartitionJoinStartDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalStartedByDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalStartsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.MillisecondsFromDayTimeDurationDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.MonthsFromYearMonthDurationDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.OverlapBinsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.ParseDateDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.ParseDateTimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.ParseTimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.PrintDateDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.PrintDateTimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.PrintTimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.TimeFromDatetimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.TimeFromUnixTimeInMsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.UnixTimeFromDateInDaysDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.UnixTimeFromDatetimeInMsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.UnixTimeFromDatetimeInSecsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.UnixTimeFromTimeInMsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.YearMonthDurationGreaterThanComparatorDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.YearMonthDurationLessThanComparatorDescriptor;
import org.apache.asterix.runtime.evaluators.staticcodegen.CodeGenUtil;
import org.apache.asterix.runtime.runningaggregates.std.TidRunningAggregateDescriptor;
import org.apache.asterix.runtime.unnestingfunctions.std.RangeDescriptor;
import org.apache.asterix.runtime.unnestingfunctions.std.ScanCollectionDescriptor;
import org.apache.asterix.runtime.unnestingfunctions.std.SubsetCollectionDescriptor;

/**
 * This class (statically) holds a list of function descriptor factories.
 */
public class FunctionCollection {

    private static final String FACTORY = "FACTORY";
    private static final List<IFunctionDescriptorFactory> temp = new ArrayList<>();

    static {
        // unnesting function
        temp.add(TidRunningAggregateDescriptor.FACTORY);
        temp.add(ScanCollectionDescriptor.FACTORY);
        temp.add(RangeDescriptor.FACTORY);
        temp.add(SubsetCollectionDescriptor.FACTORY);

        // aggregate functions
        temp.add(ListifyAggregateDescriptor.FACTORY);
        temp.add(CountAggregateDescriptor.FACTORY);
        temp.add(AvgAggregateDescriptor.FACTORY);
        temp.add(LocalAvgAggregateDescriptor.FACTORY);
        temp.add(IntermediateAvgAggregateDescriptor.FACTORY);
        temp.add(GlobalAvgAggregateDescriptor.FACTORY);
        temp.add(SumAggregateDescriptor.FACTORY);
        temp.add(LocalSumAggregateDescriptor.FACTORY);
        temp.add(MaxAggregateDescriptor.FACTORY);
        temp.add(LocalMaxAggregateDescriptor.FACTORY);
        temp.add(MinAggregateDescriptor.FACTORY);
        temp.add(LocalMinAggregateDescriptor.FACTORY);
        temp.add(FirstElementAggregateDescriptor.FACTORY);
        temp.add(LocalFirstElementAggregateDescriptor.FACTORY);

        // serializable aggregates
        temp.add(SerializableCountAggregateDescriptor.FACTORY);
        temp.add(SerializableAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableLocalAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableIntermediateAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableGlobalAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableSumAggregateDescriptor.FACTORY);
        temp.add(SerializableLocalSumAggregateDescriptor.FACTORY);

        // scalar aggregates
        temp.add(ScalarCountAggregateDescriptor.FACTORY);
        temp.add(ScalarAvgAggregateDescriptor.FACTORY);
        temp.add(ScalarSumAggregateDescriptor.FACTORY);
        temp.add(ScalarMaxAggregateDescriptor.FACTORY);
        temp.add(ScalarMinAggregateDescriptor.FACTORY);
        temp.add(EmptyStreamAggregateDescriptor.FACTORY);
        temp.add(NonEmptyStreamAggregateDescriptor.FACTORY);

        // SQL aggregates
        temp.add(SqlCountAggregateDescriptor.FACTORY);
        temp.add(SqlAvgAggregateDescriptor.FACTORY);
        temp.add(LocalSqlAvgAggregateDescriptor.FACTORY);
        temp.add(IntermediateSqlAvgAggregateDescriptor.FACTORY);
        temp.add(GlobalSqlAvgAggregateDescriptor.FACTORY);
        temp.add(SqlSumAggregateDescriptor.FACTORY);
        temp.add(LocalSqlSumAggregateDescriptor.FACTORY);
        temp.add(SqlMaxAggregateDescriptor.FACTORY);
        temp.add(LocalSqlMaxAggregateDescriptor.FACTORY);
        temp.add(SqlMinAggregateDescriptor.FACTORY);
        temp.add(LocalSqlMinAggregateDescriptor.FACTORY);

        // SQL serializable aggregates
        temp.add(SerializableSqlCountAggregateDescriptor.FACTORY);
        temp.add(SerializableSqlAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableLocalSqlAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableIntermediateSqlAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableGlobalSqlAvgAggregateDescriptor.FACTORY);
        temp.add(SerializableSqlSumAggregateDescriptor.FACTORY);
        temp.add(SerializableLocalSqlSumAggregateDescriptor.FACTORY);

        // SQL scalar aggregates
        temp.add(ScalarSqlCountAggregateDescriptor.FACTORY);
        temp.add(ScalarSqlAvgAggregateDescriptor.FACTORY);
        temp.add(ScalarSqlSumAggregateDescriptor.FACTORY);
        temp.add(ScalarSqlMaxAggregateDescriptor.FACTORY);
        temp.add(ScalarSqlMinAggregateDescriptor.FACTORY);

        // boolean functions
        temp.add(AndDescriptor.FACTORY);
        temp.add(OrDescriptor.FACTORY);

        // Record constructors
        temp.add(ClosedRecordConstructorDescriptor.FACTORY);
        temp.add(OpenRecordConstructorDescriptor.FACTORY);

        // List constructors
        temp.add(OrderedListConstructorDescriptor.FACTORY);
        temp.add(UnorderedListConstructorDescriptor.FACTORY);

        // Inject failure function
        temp.add(InjectFailureDescriptor.FACTORY);

        // Switch case
        temp.add(SwitchCaseDescriptor.FACTORY);

        // null functions
        temp.add(IsMissingDescriptor.FACTORY);
        temp.add(IsNullDescriptor.FACTORY);
        temp.add(IsUnknownDescriptor.FACTORY);
        temp.add(IsSystemNullDescriptor.FACTORY);
        temp.add(CheckUnknownDescriptor.FACTORY);

        // uuid generators (zero independent functions)
        temp.add(CreateUUIDDescriptor.FACTORY);
        temp.add(UUIDDescriptor.FACTORY);
        temp.add(CreateQueryUIDDescriptor.FACTORY);
        temp.add(CurrentDateDescriptor.FACTORY);
        temp.add(CurrentTimeDescriptor.FACTORY);
        temp.add(CurrentDateTimeDescriptor.FACTORY);

        // TODO: decide how should we deal these two weird functions as
        // the number of arguments of the function depend on the first few arguments.
        temp.add(SimilarityJaccardPrefixDescriptor.FACTORY);
        temp.add(SimilarityJaccardPrefixCheckDescriptor.FACTORY);

        // Partition functions for interval partition join pre-sorting
        temp.add(IntervalPartitionJoinStartDescriptor.FACTORY);
        temp.add(IntervalPartitionJoinEndDescriptor.FACTORY);


        // Element accessors.
        temp.add(FieldAccessByIndexDescriptor.FACTORY);
        temp.add(FieldAccessByNameDescriptor.FACTORY);
        temp.add(FieldAccessNestedDescriptor.FACTORY);
        temp.add(AnyCollectionMemberDescriptor.FACTORY);
        temp.add(GetItemDescriptor.FACTORY);

        // Numeric functions
        temp.add(NumericUnaryMinusDescriptor.FACTORY);
        temp.add(NumericAddDescriptor.FACTORY);
        temp.add(NumericDivideDescriptor.FACTORY);
        temp.add(NumericMultiplyDescriptor.FACTORY);
        temp.add(NumericSubDescriptor.FACTORY);
        temp.add(NumericModuloDescriptor.FACTORY);
        temp.add(NumericCaretDescriptor.FACTORY);
        temp.add(NotDescriptor.FACTORY);
        temp.add(LenDescriptor.FACTORY);
        temp.add(NumericAbsDescriptor.FACTORY);
        temp.add(NumericCeilingDescriptor.FACTORY);
        temp.add(NumericFloorDescriptor.FACTORY);
        temp.add(NumericRoundDescriptor.FACTORY);
        temp.add(NumericRoundHalfToEvenDescriptor.FACTORY);
        temp.add(NumericRoundHalfToEven2Descriptor.FACTORY);
        temp.add(NumericACosDescriptor.FACTORY);
        temp.add(NumericASinDescriptor.FACTORY);
        temp.add(NumericATanDescriptor.FACTORY);
        temp.add(NumericCosDescriptor.FACTORY);
        temp.add(NumericSinDescriptor.FACTORY);
        temp.add(NumericTanDescriptor.FACTORY);
        temp.add(NumericExpDescriptor.FACTORY);
        temp.add(NumericLnDescriptor.FACTORY);
        temp.add(NumericLogDescriptor.FACTORY);
        temp.add(NumericSqrtDescriptor.FACTORY);
        temp.add(NumericSignDescriptor.FACTORY);
        temp.add(NumericTruncDescriptor.FACTORY);
        temp.add(NumericATan2Descriptor.FACTORY);

        // Comparisons.
        temp.add(EqualsDescriptor.FACTORY);
        temp.add(GreaterThanDescriptor.FACTORY);
        temp.add(GreaterThanOrEqualsDescriptor.FACTORY);
        temp.add(LessThanDescriptor.FACTORY);
        temp.add(LessThanOrEqualsDescriptor.FACTORY);
        temp.add(NotEqualsDescriptor.FACTORY);

        // Binary functions
        temp.add(BinaryLengthDescriptor.FACTORY);
        temp.add(ParseBinaryDescriptor.FACTORY);
        temp.add(PrintBinaryDescriptor.FACTORY);
        temp.add(BinaryConcatDescriptor.FACTORY);
        temp.add(SubBinaryFromDescriptor.FACTORY);
        temp.add(SubBinaryFromToDescriptor.FACTORY);
        temp.add(FindBinaryDescriptor.FACTORY);
        temp.add(FindBinaryFromDescriptor.FACTORY);

        // String functions
        temp.add(StringLikeDescriptor.FACTORY);
        temp.add(StringContainsDescriptor.FACTORY);
        temp.add(StringEndsWithDescriptor.FACTORY);
        temp.add(StringStartsWithDescriptor.FACTORY);
        temp.add(SubstringDescriptor.FACTORY);
        temp.add(StringEqualDescriptor.FACTORY);
        temp.add(StringLowerCaseDescriptor.FACTORY);
        temp.add(StringUpperCaseDescriptor.FACTORY);
        temp.add(StringLengthDescriptor.FACTORY);
        temp.add(Substring2Descriptor.FACTORY);
        temp.add(SubstringBeforeDescriptor.FACTORY);
        temp.add(SubstringAfterDescriptor.FACTORY);
        temp.add(StringToCodePointDescriptor.FACTORY);
        temp.add(CodePointToStringDescriptor.FACTORY);
        temp.add(StringConcatDescriptor.FACTORY);
        temp.add(StringJoinDescriptor.FACTORY);
        temp.add(StringRegExpContainsDescriptor.FACTORY);
        temp.add(StringRegExpContainsWithFlagDescriptor.FACTORY);
        temp.add(StringRegExpLikeDescriptor.FACTORY);
        temp.add(StringRegExpLikeWithFlagDescriptor.FACTORY);
        temp.add(StringRegExpPositionDescriptor.FACTORY);
        temp.add(StringRegExpPositionWithFlagDescriptor.FACTORY);
        temp.add(StringRegExpReplaceDescriptor.FACTORY);
        temp.add(StringRegExpReplaceWithFlagsDescriptor.FACTORY);
        temp.add(StringInitCapDescriptor.FACTORY);
        temp.add(StringTrimDescriptor.FACTORY);
        temp.add(StringLTrimDescriptor.FACTORY);
        temp.add(StringRTrimDescriptor.FACTORY);
        temp.add(StringTrim2Descriptor.FACTORY);
        temp.add(StringLTrim2Descriptor.FACTORY);
        temp.add(StringRTrim2Descriptor.FACTORY);
        temp.add(StringPositionDescriptor.FACTORY);
        temp.add(StringRepeatDescriptor.FACTORY);
        temp.add(StringSplitDescriptor.FACTORY);

        // Constructors
        temp.add(ABooleanConstructorDescriptor.FACTORY);
        temp.add(ANullConstructorDescriptor.FACTORY);
        temp.add(ABinaryHexStringConstructorDescriptor.FACTORY);
        temp.add(ABinaryBase64StringConstructorDescriptor.FACTORY);
        temp.add(AStringConstructorDescriptor.FACTORY);
        temp.add(AInt8ConstructorDescriptor.FACTORY);
        temp.add(AInt16ConstructorDescriptor.FACTORY);
        temp.add(AInt32ConstructorDescriptor.FACTORY);
        temp.add(AInt64ConstructorDescriptor.FACTORY);
        temp.add(AFloatConstructorDescriptor.FACTORY);
        temp.add(ADoubleConstructorDescriptor.FACTORY);
        temp.add(APointConstructorDescriptor.FACTORY);
        temp.add(APoint3DConstructorDescriptor.FACTORY);
        temp.add(ALineConstructorDescriptor.FACTORY);
        temp.add(APolygonConstructorDescriptor.FACTORY);
        temp.add(ACircleConstructorDescriptor.FACTORY);
        temp.add(ARectangleConstructorDescriptor.FACTORY);
        temp.add(ATimeConstructorDescriptor.FACTORY);
        temp.add(ADateConstructorDescriptor.FACTORY);
        temp.add(ADateTimeConstructorDescriptor.FACTORY);
        temp.add(ADurationConstructorDescriptor.FACTORY);
        temp.add(AYearMonthDurationConstructorDescriptor.FACTORY);
        temp.add(ADayTimeDurationConstructorDescriptor.FACTORY);
        temp.add(AUUIDFromStringConstructorDescriptor.FACTORY);
        temp.add(AIntervalConstructorDescriptor.FACTORY);
        temp.add(AIntervalStartFromDateConstructorDescriptor.FACTORY);
        temp.add(AIntervalStartFromDateTimeConstructorDescriptor.FACTORY);
        temp.add(AIntervalStartFromTimeConstructorDescriptor.FACTORY);

        // Spatial
        temp.add(CreatePointDescriptor.FACTORY);
        temp.add(CreateLineDescriptor.FACTORY);
        temp.add(CreatePolygonDescriptor.FACTORY);
        temp.add(CreateCircleDescriptor.FACTORY);
        temp.add(CreateRectangleDescriptor.FACTORY);
        temp.add(SpatialAreaDescriptor.FACTORY);
        temp.add(SpatialDistanceDescriptor.FACTORY);
        temp.add(SpatialIntersectDescriptor.FACTORY);
        temp.add(CreateMBRDescriptor.FACTORY);
        temp.add(SpatialCellDescriptor.FACTORY);
        temp.add(PointXCoordinateAccessor.FACTORY);
        temp.add(PointYCoordinateAccessor.FACTORY);
        temp.add(CircleRadiusAccessor.FACTORY);
        temp.add(CircleCenterAccessor.FACTORY);
        temp.add(LineRectanglePolygonAccessor.FACTORY);

        // fuzzyjoin function
        temp.add(PrefixLenJaccardDescriptor.FACTORY);
        temp.add(WordTokensDescriptor.FACTORY);
        temp.add(HashedWordTokensDescriptor.FACTORY);
        temp.add(CountHashedWordTokensDescriptor.FACTORY);
        temp.add(GramTokensDescriptor.FACTORY);
        temp.add(HashedGramTokensDescriptor.FACTORY);
        temp.add(CountHashedGramTokensDescriptor.FACTORY);
        temp.add(EditDistanceDescriptor.FACTORY);
        temp.add(EditDistanceCheckDescriptor.FACTORY);
        temp.add(EditDistanceStringIsFilterableDescriptor.FACTORY);
        temp.add(EditDistanceListIsFilterableDescriptor.FACTORY);
        temp.add(EditDistanceContainsDescriptor.FACTORY);
        temp.add(SimilarityJaccardDescriptor.FACTORY);
        temp.add(SimilarityJaccardCheckDescriptor.FACTORY);
        temp.add(SimilarityJaccardSortedDescriptor.FACTORY);
        temp.add(SimilarityJaccardSortedCheckDescriptor.FACTORY);

        // Record functions.
        temp.add(GetRecordFieldsDescriptor.FACTORY);
        temp.add(GetRecordFieldValueDescriptor.FACTORY);
        temp.add(FieldAccessByNameDescriptor.FACTORY);
        temp.add(DeepEqualityDescriptor.FACTORY);
        temp.add(RecordMergeDescriptor.FACTORY);
        temp.add(RecordAddFieldsDescriptor.FACTORY);
        temp.add(RecordRemoveFieldsDescriptor.FACTORY);

        // Spatial and temporal type accessors
        temp.add(TemporalYearAccessor.FACTORY);
        temp.add(TemporalMonthAccessor.FACTORY);
        temp.add(TemporalDayAccessor.FACTORY);
        temp.add(TemporalHourAccessor.FACTORY);
        temp.add(TemporalMinuteAccessor.FACTORY);
        temp.add(TemporalSecondAccessor.FACTORY);
        temp.add(TemporalMillisecondAccessor.FACTORY);
        temp.add(TemporalIntervalStartAccessor.FACTORY);
        temp.add(TemporalIntervalEndAccessor.FACTORY);
        temp.add(TemporalIntervalStartDateAccessor.FACTORY);
        temp.add(TemporalIntervalEndDateAccessor.FACTORY);
        temp.add(TemporalIntervalStartTimeAccessor.FACTORY);
        temp.add(TemporalIntervalEndTimeAccessor.FACTORY);
        temp.add(TemporalIntervalStartDatetimeAccessor.FACTORY);
        temp.add(TemporalIntervalEndDatetimeAccessor.FACTORY);

        // Temporal functions
        temp.add(UnixTimeFromDateInDaysDescriptor.FACTORY);
        temp.add(UnixTimeFromTimeInMsDescriptor.FACTORY);
        temp.add(UnixTimeFromDatetimeInMsDescriptor.FACTORY);
        temp.add(UnixTimeFromDatetimeInSecsDescriptor.FACTORY);
        temp.add(DateFromUnixTimeInDaysDescriptor.FACTORY);
        temp.add(DateFromDatetimeDescriptor.FACTORY);
        temp.add(TimeFromUnixTimeInMsDescriptor.FACTORY);
        temp.add(TimeFromDatetimeDescriptor.FACTORY);
        temp.add(DatetimeFromUnixTimeInMsDescriptor.FACTORY);
        temp.add(DatetimeFromUnixTimeInSecsDescriptor.FACTORY);
        temp.add(DatetimeFromDateAndTimeDescriptor.FACTORY);
        temp.add(CalendarDurationFromDateTimeDescriptor.FACTORY);
        temp.add(CalendarDuartionFromDateDescriptor.FACTORY);
        temp.add(AdjustDateTimeForTimeZoneDescriptor.FACTORY);
        temp.add(AdjustTimeForTimeZoneDescriptor.FACTORY);
        temp.add(IntervalBeforeDescriptor.FACTORY);
        temp.add(IntervalAfterDescriptor.FACTORY);
        temp.add(IntervalMeetsDescriptor.FACTORY);
        temp.add(IntervalMetByDescriptor.FACTORY);
        temp.add(IntervalOverlapsDescriptor.FACTORY);
        temp.add(IntervalOverlappedByDescriptor.FACTORY);
        temp.add(IntervalOverlappingDescriptor.FACTORY);
        temp.add(IntervalStartsDescriptor.FACTORY);
        temp.add(IntervalStartedByDescriptor.FACTORY);
        temp.add(IntervalCoversDescriptor.FACTORY);
        temp.add(IntervalCoveredByDescriptor.FACTORY);
        temp.add(IntervalEndsDescriptor.FACTORY);
        temp.add(IntervalEndedByDescriptor.FACTORY);
        temp.add(DurationFromMillisecondsDescriptor.FACTORY);
        temp.add(DurationFromMonthsDescriptor.FACTORY);
        temp.add(YearMonthDurationGreaterThanComparatorDescriptor.FACTORY);
        temp.add(YearMonthDurationLessThanComparatorDescriptor.FACTORY);
        temp.add(DayTimeDurationGreaterThanComparatorDescriptor.FACTORY);
        temp.add(DayTimeDurationLessThanComparatorDescriptor.FACTORY);
        temp.add(MonthsFromYearMonthDurationDescriptor.FACTORY);
        temp.add(MillisecondsFromDayTimeDurationDescriptor.FACTORY);
        temp.add(DurationEqualDescriptor.FACTORY);
        temp.add(GetYearMonthDurationDescriptor.FACTORY);
        temp.add(GetDayTimeDurationDescriptor.FACTORY);
        temp.add(IntervalBinDescriptor.FACTORY);
        temp.add(OverlapBinsDescriptor.FACTORY);
        temp.add(DayOfWeekDescriptor.FACTORY);
        temp.add(ParseDateDescriptor.FACTORY);
        temp.add(ParseTimeDescriptor.FACTORY);
        temp.add(ParseDateTimeDescriptor.FACTORY);
        temp.add(PrintDateDescriptor.FACTORY);
        temp.add(PrintTimeDescriptor.FACTORY);
        temp.add(PrintDateTimeDescriptor.FACTORY);
        temp.add(GetOverlappingIntervalDescriptor.FACTORY);
        temp.add(DurationFromIntervalDescriptor.FACTORY);


        temp.add(IsBooleanDescriptor.FACTORY);
        temp.add(IsNumberDescriptor.FACTORY);
        temp.add(IsStringDescriptor.FACTORY);
        temp.add(IsArrayDescriptor.FACTORY);
        temp.add(IsObjectDescriptor.FACTORY);
// Type functions.
//        functionsToInjectUnkownHandling.add(IsBooleanDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(IsNumberDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(IsStringDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(IsArrayDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(IsObjectDescriptor.FACTORY);

        // Cast function
        temp.add(CastTypeDescriptor.FACTORY);

        temp.add(RecordPairsDescriptor.FACTORY);

//        // Record function
//        functionsToInjectUnkownHandling.add(RecordPairsDescriptor.FACTORY);
//
//        // functions that need generated class for null-handling.
//        List<IFunctionDescriptorFactory> functionsToInjectUnkownHandling = new ArrayList<>();
//
//        // Element accessors.
//        functionsToInjectUnkownHandling.add(FieldAccessByIndexDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(FieldAccessByNameDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(FieldAccessNestedDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(AnyCollectionMemberDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(GetItemDescriptor.FACTORY);
//
//        // Numeric functions
//        functionsToInjectUnkownHandling.add(NumericUnaryMinusDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NumericAddDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NumericDivideDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NumericMultiplyDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NumericSubDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NumericModuloDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NumericCaretDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NotDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(LenDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NumericAbsDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NumericCeilingDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NumericFloorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NumericRoundDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NumericRoundHalfToEvenDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NumericRoundHalfToEven2Descriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NumericACosDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NumericASinDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NumericATanDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NumericCosDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NumericSinDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NumericTanDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NumericExpDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NumericLnDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NumericLogDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NumericSqrtDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NumericSignDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NumericTruncDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NumericATan2Descriptor.FACTORY);
//
//        // Comparisons.
//        functionsToInjectUnkownHandling.add(EqualsDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(GreaterThanDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(GreaterThanOrEqualsDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(LessThanDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(LessThanOrEqualsDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(NotEqualsDescriptor.FACTORY);
//
//        // Binary functions
//        functionsToInjectUnkownHandling.add(BinaryLengthDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(ParseBinaryDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(PrintBinaryDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(BinaryConcatDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(SubBinaryFromDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(SubBinaryFromToDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(FindBinaryDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(FindBinaryFromDescriptor.FACTORY);
//
//        // String functions
//        functionsToInjectUnkownHandling.add(StringLikeDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringContainsDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringEndsWithDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringStartsWithDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(SubstringDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringEqualDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringLowerCaseDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringUpperCaseDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringLengthDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(Substring2Descriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(SubstringBeforeDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(SubstringAfterDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringToCodePointDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(CodePointToStringDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringConcatDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringJoinDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringRegExpContainsDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringRegExpContainsWithFlagDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringRegExpLikeDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringRegExpLikeWithFlagDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringRegExpPositionDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringRegExpPositionWithFlagDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringRegExpReplaceDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringRegExpReplaceWithFlagsDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringInitCapDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringTrimDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringLTrimDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringRTrimDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringTrim2Descriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringLTrim2Descriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringRTrim2Descriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringPositionDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringRepeatDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(StringSplitDescriptor.FACTORY);
//
//        // Constructors
//        functionsToInjectUnkownHandling.add(ABooleanConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(ANullConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(ABinaryHexStringConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(ABinaryBase64StringConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(AStringConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(AInt8ConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(AInt16ConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(AInt32ConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(AInt64ConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(AFloatConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(ADoubleConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(APointConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(APoint3DConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(ALineConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(APolygonConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(ACircleConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(ARectangleConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(ATimeConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(ADateConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(ADateTimeConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(ADurationConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(AYearMonthDurationConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(ADayTimeDurationConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(AUUIDFromStringConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(AIntervalConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(AIntervalStartFromDateConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(AIntervalStartFromDateTimeConstructorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(AIntervalStartFromTimeConstructorDescriptor.FACTORY);
//
//        // Spatial
//        functionsToInjectUnkownHandling.add(CreatePointDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(CreateLineDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(CreatePolygonDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(CreateCircleDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(CreateRectangleDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(SpatialAreaDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(SpatialDistanceDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(SpatialIntersectDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(CreateMBRDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(SpatialCellDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(PointXCoordinateAccessor.FACTORY);
//        functionsToInjectUnkownHandling.add(PointYCoordinateAccessor.FACTORY);
//        functionsToInjectUnkownHandling.add(CircleRadiusAccessor.FACTORY);
//        functionsToInjectUnkownHandling.add(CircleCenterAccessor.FACTORY);
//        functionsToInjectUnkownHandling.add(LineRectanglePolygonAccessor.FACTORY);
//
//        // fuzzyjoin function
//        functionsToInjectUnkownHandling.add(PrefixLenJaccardDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(WordTokensDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(HashedWordTokensDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(CountHashedWordTokensDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(GramTokensDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(HashedGramTokensDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(CountHashedGramTokensDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(EditDistanceDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(EditDistanceCheckDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(EditDistanceStringIsFilterableDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(EditDistanceListIsFilterableDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(EditDistanceContainsDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(SimilarityJaccardDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(SimilarityJaccardCheckDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(SimilarityJaccardSortedDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(SimilarityJaccardSortedCheckDescriptor.FACTORY);
//
//        // Record functions.
//        functionsToInjectUnkownHandling.add(GetRecordFieldsDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(GetRecordFieldValueDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(FieldAccessByNameDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(DeepEqualityDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(RecordMergeDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(RecordAddFieldsDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(RecordRemoveFieldsDescriptor.FACTORY);
//
//        // Spatial and temporal type accessors
//        functionsToInjectUnkownHandling.add(TemporalYearAccessor.FACTORY);
//        functionsToInjectUnkownHandling.add(TemporalMonthAccessor.FACTORY);
//        functionsToInjectUnkownHandling.add(TemporalDayAccessor.FACTORY);
//        functionsToInjectUnkownHandling.add(TemporalHourAccessor.FACTORY);
//        functionsToInjectUnkownHandling.add(TemporalMinuteAccessor.FACTORY);
//        functionsToInjectUnkownHandling.add(TemporalSecondAccessor.FACTORY);
//        functionsToInjectUnkownHandling.add(TemporalMillisecondAccessor.FACTORY);
//        functionsToInjectUnkownHandling.add(TemporalIntervalStartAccessor.FACTORY);
//        functionsToInjectUnkownHandling.add(TemporalIntervalEndAccessor.FACTORY);
//        functionsToInjectUnkownHandling.add(TemporalIntervalStartDateAccessor.FACTORY);
//        functionsToInjectUnkownHandling.add(TemporalIntervalEndDateAccessor.FACTORY);
//        functionsToInjectUnkownHandling.add(TemporalIntervalStartTimeAccessor.FACTORY);
//        functionsToInjectUnkownHandling.add(TemporalIntervalEndTimeAccessor.FACTORY);
//        functionsToInjectUnkownHandling.add(TemporalIntervalStartDatetimeAccessor.FACTORY);
//        functionsToInjectUnkownHandling.add(TemporalIntervalEndDatetimeAccessor.FACTORY);
//
//        // Temporal functions
//        functionsToInjectUnkownHandling.add(UnixTimeFromDateInDaysDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(UnixTimeFromTimeInMsDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(UnixTimeFromDatetimeInMsDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(UnixTimeFromDatetimeInSecsDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(DateFromUnixTimeInDaysDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(DateFromDatetimeDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(TimeFromUnixTimeInMsDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(TimeFromDatetimeDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(DatetimeFromUnixTimeInMsDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(DatetimeFromUnixTimeInSecsDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(DatetimeFromDateAndTimeDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(CalendarDurationFromDateTimeDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(CalendarDuartionFromDateDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(AdjustDateTimeForTimeZoneDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(AdjustTimeForTimeZoneDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(IntervalBeforeDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(IntervalAfterDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(IntervalMeetsDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(IntervalMetByDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(IntervalOverlapsDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(IntervalOverlappedByDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(IntervalOverlappingDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(IntervalStartsDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(IntervalStartedByDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(IntervalCoversDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(IntervalCoveredByDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(IntervalEndsDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(IntervalEndedByDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(DurationFromMillisecondsDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(DurationFromMonthsDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(YearMonthDurationGreaterThanComparatorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(YearMonthDurationLessThanComparatorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(DayTimeDurationGreaterThanComparatorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(DayTimeDurationLessThanComparatorDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(MonthsFromYearMonthDurationDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(MillisecondsFromDayTimeDurationDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(DurationEqualDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(GetYearMonthDurationDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(GetDayTimeDurationDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(IntervalBinDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(OverlapBinsDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(DayOfWeekDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(ParseDateDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(ParseTimeDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(ParseDateTimeDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(PrintDateDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(PrintTimeDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(PrintDateTimeDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(GetOverlappingIntervalDescriptor.FACTORY);
//        functionsToInjectUnkownHandling.add(DurationFromIntervalDescriptor.FACTORY);
//
//        // Cast function
//        functionsToInjectUnkownHandling.add(CastTypeDescriptor.FACTORY);
//
//        List<IFunctionDescriptorFactory> generatedFactories = new ArrayList<>();
//        for (IFunctionDescriptorFactory factory : functionsToInjectUnkownHandling) {
//            generatedFactories
//                    .add(getGeneratedFunctionDescriptorFactory(factory.createFunctionDescriptor().getClass()));
//        }
//        temp.addAll(generatedFactories);
    }

    public static List<IFunctionDescriptorFactory> getFunctionDescriptorFactories() {
        return temp;
    }

    /**
     * Gets the generated function descriptor factory from an <code>IFunctionDescriptor</code>
     * implementation class.
     *
     * @param cl,
     *            the class of an <code>IFunctionDescriptor</code> implementation.
     * @return the IFunctionDescriptorFactory instance defined in the class.
     */
    private static IFunctionDescriptorFactory getGeneratedFunctionDescriptorFactory(Class<?> cl) {
        try {
            String className = CodeGenUtil.getGeneratedFunctionDescriptorClassName(cl.getName(),
                    CodeGenUtil.DEFAULT_SUFFIX_FOR_GENERATED_CLASS);
            Class<?> generatedCl = cl.getClassLoader().loadClass(className);
            Field factory = generatedCl.getDeclaredField(FACTORY);
            return (IFunctionDescriptorFactory) factory.get(null);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

}
