package goavro

import (
	"encoding/json"
	"fmt"
)

type SymbolTable struct {
	codecsByFullName map[string]*Codec
}

func NewSymbolTable() *SymbolTable {
	return &SymbolTable{builtinSymbolTable()}
}

func (schemas *SymbolTable) NewCodec(schemaSpecification string) (*Codec, error) {
	var schema interface{}

	if err := json.Unmarshal([]byte(schemaSpecification), &schema); err != nil {
		return nil, fmt.Errorf("cannot unmarshal schema JSON: %s", err)
	}

	c, err := buildCodec(schemas.codecsByFullName, nullNamespace, schema)
	if err == nil {
		// // compact schema and save it
		// compact, err := json.Marshal(schema)
		// if err != nil {
		// 	return nil, fmt.Errorf("cannot remarshal schema: %s", err)
		// }
		// c.schemaOriginal = string(compact)
		c.schemaOriginal = schemaSpecification
		c.schemaCanonical = parsingCanonicalForm(schema)
	}

	return c, err
}

func builtinSymbolTable() map[string]*Codec {
	return map[string]*Codec{
		"boolean": {
			typeName:          &name{"boolean", nullNamespace},
			schemaOriginal:    "boolean",
			schemaCanonical:   "boolean",
			binaryFromNative:  booleanBinaryFromNative,
			nativeFromBinary:  booleanNativeFromBinary,
			nativeFromTextual: booleanNativeFromTextual,
			textualFromNative: booleanTextualFromNative,
		},
		"bytes": {
			typeName:          &name{"bytes", nullNamespace},
			schemaOriginal:    "bytes",
			schemaCanonical:   "bytes",
			binaryFromNative:  bytesBinaryFromNative,
			nativeFromBinary:  bytesNativeFromBinary,
			nativeFromTextual: bytesNativeFromTextual,
			textualFromNative: bytesTextualFromNative,
		},
		"double": {
			typeName:          &name{"double", nullNamespace},
			schemaOriginal:    "double",
			schemaCanonical:   "double",
			binaryFromNative:  doubleBinaryFromNative,
			nativeFromBinary:  doubleNativeFromBinary,
			nativeFromTextual: doubleNativeFromTextual,
			textualFromNative: doubleTextualFromNative,
		},
		"float": {
			typeName:          &name{"float", nullNamespace},
			schemaOriginal:    "float",
			schemaCanonical:   "float",
			binaryFromNative:  floatBinaryFromNative,
			nativeFromBinary:  floatNativeFromBinary,
			nativeFromTextual: floatNativeFromTextual,
			textualFromNative: floatTextualFromNative,
		},
		"int": {
			typeName:          &name{"int", nullNamespace},
			schemaOriginal:    "int",
			schemaCanonical:   "int",
			binaryFromNative:  intBinaryFromNative,
			nativeFromBinary:  intNativeFromBinary,
			nativeFromTextual: intNativeFromTextual,
			textualFromNative: intTextualFromNative,
		},
		"long": {
			typeName:          &name{"long", nullNamespace},
			schemaOriginal:    "long",
			schemaCanonical:   "long",
			binaryFromNative:  longBinaryFromNative,
			nativeFromBinary:  longNativeFromBinary,
			nativeFromTextual: longNativeFromTextual,
			textualFromNative: longTextualFromNative,
		},
		"null": {
			typeName:          &name{"null", nullNamespace},
			schemaOriginal:    "null",
			schemaCanonical:   "null",
			binaryFromNative:  nullBinaryFromNative,
			nativeFromBinary:  nullNativeFromBinary,
			nativeFromTextual: nullNativeFromTextual,
			textualFromNative: nullTextualFromNative,
		},
		"string": {
			typeName:          &name{"string", nullNamespace},
			schemaOriginal:    "string",
			schemaCanonical:   "string",
			binaryFromNative:  stringBinaryFromNative,
			nativeFromBinary:  stringNativeFromBinary,
			nativeFromTextual: stringNativeFromTextual,
			textualFromNative: stringTextualFromNative,
		},
		// Start of compiled logical types using format typeName.logicalType where there is
		// no dependence on schema.
		"long.timestamp-millis": {
			typeName:          &name{"long.timestamp-millis", nullNamespace},
			schemaOriginal:    "long",
			schemaCanonical:   "long",
			nativeFromTextual: nativeFromTimeStampMillis(longNativeFromTextual),
			binaryFromNative:  timeStampMillisFromNative(longBinaryFromNative),
			nativeFromBinary:  nativeFromTimeStampMillis(longNativeFromBinary),
			textualFromNative: timeStampMillisFromNative(longTextualFromNative),
		},
		"long.timestamp-micros": {
			typeName:          &name{"long.timestamp-micros", nullNamespace},
			schemaOriginal:    "long",
			schemaCanonical:   "long",
			nativeFromTextual: nativeFromTimeStampMicros(longNativeFromTextual),
			binaryFromNative:  timeStampMicrosFromNative(longBinaryFromNative),
			nativeFromBinary:  nativeFromTimeStampMicros(longNativeFromBinary),
			textualFromNative: timeStampMicrosFromNative(longTextualFromNative),
		},
		"int.time-millis": {
			typeName:          &name{"int.time-millis", nullNamespace},
			schemaOriginal:    "int",
			schemaCanonical:   "int",
			nativeFromTextual: nativeFromTimeMillis(intNativeFromTextual),
			binaryFromNative:  timeMillisFromNative(intBinaryFromNative),
			nativeFromBinary:  nativeFromTimeMillis(intNativeFromBinary),
			textualFromNative: timeMillisFromNative(intTextualFromNative),
		},
		"long.time-micros": {
			typeName:          &name{"long.time-micros", nullNamespace},
			schemaOriginal:    "long",
			schemaCanonical:   "long",
			nativeFromTextual: nativeFromTimeMicros(longNativeFromTextual),
			binaryFromNative:  timeMicrosFromNative(longBinaryFromNative),
			nativeFromBinary:  nativeFromTimeMicros(longNativeFromBinary),
			textualFromNative: timeMicrosFromNative(longTextualFromNative),
		},
		"int.date": {
			typeName:          &name{"int.date", nullNamespace},
			schemaOriginal:    "int",
			schemaCanonical:   "int",
			nativeFromTextual: nativeFromDate(intNativeFromTextual),
			binaryFromNative:  dateFromNative(intBinaryFromNative),
			nativeFromBinary:  nativeFromDate(intNativeFromBinary),
			textualFromNative: dateFromNative(intTextualFromNative),
		},
	}
}
