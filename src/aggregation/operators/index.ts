/**
 * Operator registry - maps operator names to handler functions.
 */
import * as arithmetic from "./arithmetic.ts";
import * as string from "./string.ts";
import * as comparison from "./comparison.ts";
import * as conditional from "./conditional.ts";
import * as array from "./array.ts";
import * as typeConversion from "./type-conversion.ts";
import * as date from "./date.ts";

/**
 * Registry of all expression operators.
 * Each operator is a function that takes (args, doc, vars, evaluate) and returns a value.
 */
export const operators = {
  // Arithmetic
  $add: arithmetic.evalAdd,
  $subtract: arithmetic.evalSubtract,
  $multiply: arithmetic.evalMultiply,
  $divide: arithmetic.evalDivide,
  $abs: arithmetic.evalAbs,
  $ceil: arithmetic.evalCeil,
  $floor: arithmetic.evalFloor,
  $round: arithmetic.evalRound,
  $mod: arithmetic.evalMod,
  $rand: arithmetic.evalRand,
  $exp: arithmetic.evalExp,
  $ln: arithmetic.evalLn,
  $log: arithmetic.evalLog,
  $log10: arithmetic.evalLog10,
  $pow: arithmetic.evalPow,
  $sqrt: arithmetic.evalSqrt,
  $trunc: arithmetic.evalTrunc,

  // String
  $concat: string.evalConcat,
  $toUpper: string.evalToUpper,
  $toLower: string.evalToLower,
  $substrCP: string.evalSubstrCP,
  $strLenCP: string.evalStrLenCP,
  $split: string.evalSplit,
  $trim: string.evalTrim,
  $ltrim: string.evalLTrim,
  $rtrim: string.evalRTrim,
  $toString: string.evalToString,
  $indexOfCP: string.evalIndexOfCP,
  $regexFind: string.evalRegexFind,
  $regexFindAll: string.evalRegexFindAll,
  $regexMatch: string.evalRegexMatch,
  $replaceOne: string.evalReplaceOne,
  $replaceAll: string.evalReplaceAll,
  $strcasecmp: string.evalStrcasecmp,
  $strLenBytes: string.evalStrLenBytes,
  $indexOfBytes: string.evalIndexOfBytes,
  $substrBytes: string.evalSubstrBytes,

  // Comparison
  $gt: comparison.evalGt,
  $gte: comparison.evalGte,
  $lt: comparison.evalLt,
  $lte: comparison.evalLte,
  $eq: comparison.evalEq,
  $ne: comparison.evalNe,
  $cmp: comparison.evalCmp,

  // Conditional
  $cond: conditional.evalCond,
  $ifNull: conditional.evalIfNull,
  $switch: conditional.evalSwitch,

  // Array
  $size: array.evalSize,
  $arrayElemAt: array.evalArrayElemAt,
  $slice: array.evalSlice,
  $concatArrays: array.evalConcatArrays,
  $filter: array.evalFilter,
  $map: array.evalMap,
  $reduce: array.evalReduce,
  $in: array.evalIn,
  $first: array.evalFirst,
  $last: array.evalLast,
  $indexOfArray: array.evalIndexOfArray,
  $isArray: array.evalIsArray,
  $range: array.evalRange,
  $reverseArray: array.evalReverseArray,
  $arrayToObject: array.evalArrayToObject,
  $objectToArray: array.evalObjectToArray,
  $zip: array.evalZip,
  $sortArray: array.evalSortArray,

  // Type conversion
  $toInt: typeConversion.evalToInt,
  $toDouble: typeConversion.evalToDouble,
  $toBool: typeConversion.evalToBool,
  $toDate: typeConversion.evalToDate,
  $type: typeConversion.evalType,
  $isNumber: typeConversion.evalIsNumber,
  $toLong: typeConversion.evalToLong,
  $toDecimal: typeConversion.evalToDecimal,
  $toObjectId: typeConversion.evalToObjectId,
  $convert: typeConversion.evalConvert,

  // Date
  $year: date.evalYear,
  $month: date.evalMonth,
  $dayOfMonth: date.evalDayOfMonth,
  $hour: date.evalHour,
  $minute: date.evalMinute,
  $second: date.evalSecond,
  $dayOfWeek: date.evalDayOfWeek,
  $dateToString: date.evalDateToString,
  $millisecond: date.evalMillisecond,
  $dayOfYear: date.evalDayOfYear,
  $week: date.evalWeek,
  $isoWeek: date.evalIsoWeek,
  $isoWeekYear: date.evalIsoWeekYear,
  $isoDayOfWeek: date.evalIsoDayOfWeek,
  $dateAdd: date.evalDateAdd,
  $dateSubtract: date.evalDateSubtract,
  $dateDiff: date.evalDateDiff,
  $dateFromParts: date.evalDateFromParts,
  $dateToParts: date.evalDateToParts,
  $dateFromString: date.evalDateFromString,
};
