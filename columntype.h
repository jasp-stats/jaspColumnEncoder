#ifndef COLUMNTYPE_H
#define COLUMNTYPE_H
#include "enumutilities.h"

DECLARE_ENUM(columnType,				unknown = 0, scale = 1, ordinal = 2, nominal = 3, nominalText = 4);
DECLARE_ENUM(columnTypeChangeResult,	changed, cannotConvertStringValueToInteger, cannotConvertStringValueToDouble, cannotConvertDoubleValueToInteger, generatedFromAnalysis, unknownError);
DECLARE_ENUM(computedColumnType,		notComputed, rCode, constructorCode, analysis, analysisNotComputed);
DECLARE_ENUM(dbDbl,						nan, inf, neg_inf);

#endif // COLUMNTYPE_H
