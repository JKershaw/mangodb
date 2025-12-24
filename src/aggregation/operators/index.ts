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

  // Comparison
  $gt: comparison.evalGt,
  $gte: comparison.evalGte,
  $lt: comparison.evalLt,
  $lte: comparison.evalLte,
  $eq: comparison.evalEq,
  $ne: comparison.evalNe,

  // Conditional
  $cond: conditional.evalCond,
  $ifNull: conditional.evalIfNull,

  // Array
  $size: array.evalSize,
  $arrayElemAt: array.evalArrayElemAt,
  $slice: array.evalSlice,
  $concatArrays: array.evalConcatArrays,
  $filter: array.evalFilter,
  $map: array.evalMap,
  $reduce: array.evalReduce,
  $in: array.evalIn,

  // Type conversion
  $toInt: typeConversion.evalToInt,
  $toDouble: typeConversion.evalToDouble,
  $toBool: typeConversion.evalToBool,
  $toDate: typeConversion.evalToDate,
  $type: typeConversion.evalType,

  // Date
  $year: date.evalYear,
  $month: date.evalMonth,
  $dayOfMonth: date.evalDayOfMonth,
  $hour: date.evalHour,
  $minute: date.evalMinute,
  $second: date.evalSecond,
  $dayOfWeek: date.evalDayOfWeek,
  $dateToString: date.evalDateToString,
};
