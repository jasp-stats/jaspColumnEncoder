//
// Copyright (C) 2013-2018 University of Amsterdam
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
//

#include "columnencoder.h"
#include "stringutils.h"
#include <regex>
#ifdef BUILDING_JASP
#include "log.h"
#define LOGGER Log::log()
#else
#include <Rcpp.h>
#define LOGGER Rcpp::Rcout
#endif



ColumnEncoder				*	ColumnEncoder::_columnEncoder				= nullptr;
std::set<ColumnEncoder*>	*	ColumnEncoder::_otherEncoders				= nullptr;
bool							ColumnEncoder::_encodingMapInvalidated		= true;
bool							ColumnEncoder::_decodingMapInvalidated		= true;
bool							ColumnEncoder::_decodingTypeInvalidated		= true;
bool							ColumnEncoder::_decoSafeMapInvalidated		= true;
bool							ColumnEncoder::_originalNamesInvalidated	= true;
bool							ColumnEncoder::_encodedNamesInvalidated		= true;


ColumnEncoder * ColumnEncoder::columnEncoder()
{
	if(!_columnEncoder)
		_columnEncoder = new ColumnEncoder();

	return _columnEncoder;
}

void ColumnEncoder::invalidateAll()
{
	_encodingMapInvalidated		= true;
	_decodingMapInvalidated		= true;
	_decodingTypeInvalidated	= true;
	_decoSafeMapInvalidated		= true;
	_originalNamesInvalidated	= true;
	_encodedNamesInvalidated	= true;
}

ColumnEncoder::ColumnEncoder(std::string prefix, std::string postfix)
	: _encodePrefix(prefix), _encodePostfix(postfix)
{
	if(!_otherEncoders)
	{
		_otherEncoders = new ColumnEncoder::ColumnEncoders();
		invalidateAll();
	}

	_otherEncoders->insert(this);
}

ColumnEncoder::ColumnEncoder(const std::map<std::string, std::string> & decodeDifferently)
	: _encodePrefix("JASPColumn_"), _encodePostfix("_For_Replacement")
{

	std::vector<std::string> originalNames;
	originalNames.reserve(decodeDifferently.size());

	for(const auto & oriNew : decodeDifferently)
		originalNames.push_back(oriNew.first);

	setCurrentNames(originalNames);

	for(const std::string & encodedName : _encodedNames)
		if(decodeDifferently.count(_decodingMap[encodedName]) > 0)
			_decodingMap[encodedName] = decodeDifferently.at(_decodingMap[encodedName]);
}

ColumnEncoder::~ColumnEncoder()
{
	if(this != _columnEncoder)
	{
		if(_otherEncoders && _otherEncoders->count(this) > 0) //The special "replacer-encoder" doesn't add itself to otherEncoders.
			_otherEncoders->erase(this);
	}
	else
	{
		_columnEncoder = nullptr;

		ColumnEncoders others = *_otherEncoders;

		for(ColumnEncoder * colEnc : others)
			delete colEnc;

		if(_otherEncoders->size() > 0)
			LOGGER << "Something went wrong removing other ColumnEncoders..." << std::endl;

		delete _otherEncoders;
		_otherEncoders = nullptr;

		invalidateAll();
	}
}

std::string ColumnEncoder::encode(const std::string &in)
{
	if(in == "") return "";

	if(encodingMap().count(in) == 0)
		throw std::runtime_error("Trying to encode columnName but '" + in + "' is not a columnName!");

	return encodingMap().at(in);
}

std::string ColumnEncoder::decode(const std::string &in)
{
	if(in == "") return "";

	if(decodingMap().count(in) == 0)
		throw std::runtime_error("Trying to decode columnName but '" + in + "' is not an encoded columnName!");

	return decodingMap().at(in);
}

columnType ColumnEncoder::columnTypeFromEncoded(const std::string &in)
{
	if(in == "" || decodingTypes().count(in) == 0) 
		return columnType::unknown;
	
	return decodingTypes().at(in);
}

void ColumnEncoder::setCurrentNames(const std::vector<std::string> & names, bool generateTypesEncoding)
{
	//LOGGER << "ColumnEncoder::setCurrentNames(#"<< names.size() << ")" << std::endl;

	_encodingMap.clear();
	_decodingMap.clear();

	_encodedNames.clear();
	_encodedNames.reserve(names.size());
	
	size_t runningCounter = 0;
	
	_originalNames = names;

	//First normal encoding decoding: (Although im not sure we would ever need those again?)
	for(size_t col = 0; col < names.size(); col++)
	{
		std::string newName			= _encodePrefix + std::to_string(runningCounter++) + _encodePostfix; //Slightly weird (but R-syntactically valid) name to avoid collisions with user stuff.
		_encodingMap[names[col]]	= newName;
		_decodingMap[newName]		= names[col];

		_encodedNames.push_back(newName);
	}
	
	if(generateTypesEncoding)
		for(size_t col = 0; col < names.size(); col++)
			for(columnType colType : { columnType::scale, columnType::ordinal, columnType::nominal })
				{
					std::string qualifiedName	= names[col] + "." + columnTypeToString(colType),
								newName			= _encodePrefix + std::to_string(runningCounter++) + _encodePostfix; //Slightly weird (but R-syntactically valid) name to avoid collisions with user stuff.
					_encodingMap[qualifiedName]	= newName;
					_decodingMap[newName]		= names[col]; //Decoding is back to the actual name in the data!
					_decodingTypes[newName]		= colType;
			
					_encodedNames	.push_back(newName);
					_originalNames	.push_back(qualifiedName);
				}

	
	sortVectorBigToSmall(_originalNames);
	invalidateAll();
}

void ColumnEncoder::sortVectorBigToSmall(std::vector<std::string> & vec)
{
	std::sort(vec.begin(), vec.end(), [](std::string & a, std::string & b) { return a.size() > b.size(); }); //We need this to make sure smaller columnNames do not bite chunks off of larger ones
}

const ColumnEncoder::colMap	&	ColumnEncoder::encodingMap()
{
	static ColumnEncoder::colMap map;

	if(_encodingMapInvalidated)
	{
		map = _columnEncoder->_encodingMap;

		if(_otherEncoders)
			for(const ColumnEncoder * other : *_otherEncoders)
				for(const auto & keyVal : other->_encodingMap)
					if(map.count(keyVal.first) == 0)
						map[keyVal.first] = keyVal.second;

		_encodingMapInvalidated = false;
	}

	return map;
}

const ColumnEncoder::colMap	&	ColumnEncoder::decodingMap()
{
	static ColumnEncoder::colMap map;

	if(_decodingMapInvalidated)
	{
		map = _columnEncoder->_decodingMap;

		if(_otherEncoders)
			for(const ColumnEncoder * other : *_otherEncoders)
				for(const auto & keyVal : other->_decodingMap)
					if(map.count(keyVal.first) == 0)
						map[keyVal.first] = keyVal.second;

		_decodingMapInvalidated = false;
	}

	return map;
}

const ColumnEncoder::colTypeMap &ColumnEncoder::decodingTypes()
{
	static ColumnEncoder::colTypeMap map;

	if(_decodingTypeInvalidated)
	{
		map = _columnEncoder->_decodingTypes;

		if(_otherEncoders)
			for(const ColumnEncoder * other : *_otherEncoders)
				for(const auto & keyVal : other->_decodingTypes)
					if(map.count(keyVal.first) == 0)
						map[keyVal.first] = keyVal.second;

		_decodingTypeInvalidated = false;
	}

	return map;
}


const ColumnEncoder::colMap	&	ColumnEncoder::decodingMapSafeHtml()
{
	static ColumnEncoder::colMap map;

	if(_decoSafeMapInvalidated)
	{
		map.clear();
		
		for(const auto & keyVal : _columnEncoder->_decodingMap)
			if(map.count(keyVal.first) == 0)
				map[keyVal.first] = stringUtils::escapeHtmlStuff(keyVal.second, true);

		if(_otherEncoders)
			for(const ColumnEncoder * other : *_otherEncoders)
				for(const auto & keyVal : other->_decodingMap)
					if(map.count(keyVal.first) == 0)
						map[keyVal.first] = stringUtils::escapeHtmlStuff(keyVal.second, true); // replace square brackets for https://github.com/jasp-stats/jasp-issues/issues/2625

		_decoSafeMapInvalidated = false;
	}

	return map;
}

const ColumnEncoder::colVec	&	ColumnEncoder::originalNames()
{
	static ColumnEncoder::colVec vec;

	if(_originalNamesInvalidated)
	{
		vec = _columnEncoder->_originalNames;

		if(_otherEncoders)
			for(const ColumnEncoder * other : *_otherEncoders)
				for(const std::string & name : other->_originalNames)
					vec.push_back(name);

		_originalNamesInvalidated = false;
	}

	sortVectorBigToSmall(vec);

	return vec;
}

const ColumnEncoder::colVec	&	ColumnEncoder::encodedNames()
{
	static ColumnEncoder::colVec vec;

	if(_encodedNamesInvalidated)
	{
		vec = _columnEncoder->_encodedNames;

		if(_otherEncoders)
			for(const ColumnEncoder * other : *_otherEncoders)
				for(const std::string & name : other->_encodedNames)
					vec.push_back(name);

		_encodedNamesInvalidated = false;
	}

	sortVectorBigToSmall(vec);

	return vec;
}

bool ColumnEncoder::shouldEncode(const std::string & in)
{
	return _encodingMap.count(in) > 0;
}

bool ColumnEncoder::shouldDecode(const std::string & in)
{
	return _decodingMap.count(in) > 0;
}

std::string	ColumnEncoder::replaceAllStrict(const std::string & text, const std::map<std::string, std::string> & map)
{
	if (map.count(text) > 0)
		return map.at(text);
	else
		return text;
}

std::string	ColumnEncoder::replaceAll(std::string text, const std::map<std::string, std::string> & map, const std::vector<std::string> & names)
{
	size_t foundPos = 0;

	while(foundPos < std::string::npos)
	{
		size_t firstFoundPos	= std::string::npos;

		std::string replaceThis;

		//First we find the first occurence of a replaceable text.
		for(const std::string & replaceMe : names) //We follow names instead of keyvals from map because they ought to be sorted from largest to smallest string (_originalNames) to not make sub-replacements
		{
			size_t pos = text.find(replaceMe, foundPos);
			if(pos < firstFoundPos)
			{
				firstFoundPos = pos;
				replaceThis = replaceMe;
			}
		}

		//We found something to replace and this will be the first occurence of anything like that. Replace it!
		if(firstFoundPos != std::string::npos)
		{
			foundPos = firstFoundPos;
			const std::string & replacement = map.at(replaceThis);
			text.replace(foundPos, replaceThis.length(), replacement);
			foundPos += replacement.length(); //Let's make sure we start replacing from after where we just replaced
		}
		else
			foundPos = std::string::npos;
	}

	return text;
}

std::string ColumnEncoder::encodeRScript(std::string text, std::set<std::string> * columnNamesFound)
{
	return encodeRScript(text, encodingMap(), originalNames(), columnNamesFound);
}

/*!
 * \brief Replace column names with encoded column names.
 * \param allowedPrefixes Specifies all prefix that column names may have and still be seen as columns names. e.g. "data.<column_name>"
 * \return Original string with column names replaced by encoded column names
 */
std::string ColumnEncoder::encodeRScript(std::string text, std::map<std::string, std::set<std::string>>& prefixedColumnsFound, const std::set<std::string>& allowedPrefixes)
{
	stringvec prefixes(allowedPrefixes.begin(), allowedPrefixes.end());
	
	std::sort(prefixes.begin(), prefixes.end(), [](const std::string & l, const std::string & r){ return l.size() < r.size();  });
	prefixes.insert(prefixes.begin(), ""); //empty first
	
	prefixedColumnsFound.clear();
	
	for(auto& prefix : prefixes) {
		stringset columnNamesFound;
		text = encodeRScript(text, encodingMap(), originalNames(), &columnNamesFound, prefix);
		prefixedColumnsFound.insert({prefix, columnNamesFound});
	}
	return text;
}

/*!
 * \brief Replace column names with encoded column names.
 * \param mandatoryPrefix Specifies a prefix that column names SHOULD have in order to be replaced/encoded. We skip all names matches that lack this prefix. Default ""/empty_string means that all variables will be replaced.
 * \return Original string with column names replaced by encoded column names
 */
std::string ColumnEncoder::encodeRScript(std::string text, const std::map<std::string, std::string> & map, const std::vector<std::string> & names, std::set<std::string> * columnNamesFound, const std::string& mandatoryPrefix)
{
	if(columnNamesFound)
		columnNamesFound->clear();

	static std::regex nonNameChar("[^\\.A-Za-z0-9_]");

	//Some lambas to test if Column names matches are free and not just a substr of some other expression.
	auto testStartWhiteSpace = [&](size_t pos) -> bool
	{
		return pos == 0 || std::regex_match(text.substr(pos - 1, 1),	nonNameChar);
	};
	
	auto testFreePrefix = [&](size_t pos) -> bool
	{
		if(mandatoryPrefix == "") return false;
		try {
			bool match = text.substr(pos - mandatoryPrefix.length(), mandatoryPrefix.length()) == mandatoryPrefix;
			if(match) return testStartWhiteSpace(pos - mandatoryPrefix.length()); //Must still be free before prefix
		}
		catch(std::out_of_range e){}
		return false;
	};
	
	auto testEndFree = [&](size_t pos) -> bool
	{
		bool endIsFree = pos == text.length() || std::regex_match(text.substr(pos, 1),	nonNameChar);

		//Check for "(" as well because maybe someone has a columnname such as rep or if or something weird like that. This might however have some whitespace in between...
		bool keepGoing = true;
		for(size_t bracePos = pos; bracePos < text.size() && endIsFree && keepGoing; bracePos++)
			if(text[bracePos] == '(')
				endIsFree = false;
			else if(text[bracePos] != '\t' && text[bracePos] != ' ')
				keepGoing = false; //Aka something else than whitespace or a brace and that means that we can replace it!
		return endIsFree;
	};

	//for now we simply replace any found columnname by its encoded variant if found
	for(const std::string & oldCol : names)
	{
		std::string	newCol	= map.at(oldCol);

		std::vector<size_t> foundColPositions = getPositionsColumnNameMatches(text, oldCol);
		std::reverse(foundColPositions.begin(), foundColPositions.end());

		for (size_t foundPos : foundColPositions)
		{
			size_t foundPosEnd = foundPos + oldCol.length();

			bool hasFreePrefix = testFreePrefix(foundPos);
			if(mandatoryPrefix != "" && !hasFreePrefix) // simply skip if the var did not have mandatoryPrefix
				continue;

			//check if it is a "free columnname" aka is there some space or a kind in front of it. We would not want to replace a part of another term (Imagine what happens when you use a columname such as "E" and a filter that includes the term TRUE, it does not end well..)
			bool startIsFree	= hasFreePrefix || testStartWhiteSpace(foundPos);
			bool endIsFree		= testEndFree(foundPosEnd);

			if(startIsFree && endIsFree)
			{
				text.replace(foundPos, oldCol.length(), newCol);
				if(columnNamesFound)
					columnNamesFound->insert(oldCol);
			}
		}
	}
	return text;
}

std::vector<size_t> ColumnEncoder::getPositionsColumnNameMatches(const std::string & text, const std::string & columnName)
{
	std::vector<size_t> positions;

	bool inString	= false;
	char delim		= '?';

	for (std::string::size_type pos = 0; pos < text.length(); ++pos)
		if (!inString && text.substr(pos, columnName.length()) == columnName)
			positions.push_back(int(pos));
		else if (text[pos] == '"' || text[pos] == '\'') //string starts or ends. This does not take into account escape characters though...
		{
			if (!inString)
			{
				delim		= text[pos];
				inString	= true;
			}
			else if(text[pos] == delim)
				inString = false;
		}

	return positions;
}


void ColumnEncoder::encodeJson(Json::Value & json, bool replaceNames, bool replaceStrict)
{
	//std::cout << "Json before encoding:\n" << json.toStyledString();
	replaceAll(json, encodingMap(), originalNames(), replaceNames, replaceStrict);
	//std::cout << "Json after encoding:\n" << json.toStyledString() << std::endl;
}

void ColumnEncoder::decodeJson(Json::Value & json, bool replaceNames)
{
	//std::cout << "Json before encoding:\n" << json.toStyledString();
	replaceAll(json, decodingMap(), encodedNames(), replaceNames, false);
	//std::cout << "Json after encoding:\n" << json.toStyledString() << std::endl;
}

void ColumnEncoder::decodeJsonSafeHtml(Json::Value & json)
{
	replaceAll(json, decodingMapSafeHtml(), encodedNames(), true, false);
}


void ColumnEncoder::replaceAll(Json::Value & json, const std::map<std::string, std::string> & map, const std::vector<std::string> & names, bool replaceNames, bool replaceStrict)
{
	switch(json.type())
	{
	case Json::arrayValue:
		for(Json::Value & option : json)
			replaceAll(option, map, names, replaceNames, replaceStrict);
		return;

	case Json::objectValue:
	{
		std::map<std::string, std::string> changedMembers;

		for(const std::string & optionName : json.getMemberNames())
		{
			replaceAll(json[optionName], map, names, replaceNames, replaceStrict);

			if(replaceNames)
			{
				std::string replacedName = replaceStrict ? replaceAllStrict(optionName, map) : replaceAll(optionName, map, names);

				if(replacedName != optionName)
					changedMembers[optionName] = replacedName;
			}
		}

		for(const auto & origNew : changedMembers) //map is empty if !replaceNames
		{
			json[origNew.second] = json[origNew.first];
			json.removeMember(origNew.first);
		}

		return;
	}

	case Json::stringValue:
		json = replaceStrict ? replaceAllStrict(json.asString(), map) : replaceAll(json.asString(), map, names);
		return;

	default:
		return;
	}
}

void ColumnEncoder::setCurrentNamesFromOptionsMeta(const Json::Value & options)
{
	std::vector<std::string> namesFound;

	if(!options.isNull() && options.isMember(".meta"))
		collectExtraEncodingsFromMetaJson(options[".meta"], namesFound);

	setCurrentNames(namesFound);
}

void ColumnEncoder::setCurrentColumnTypePerName(const colTypeMap &theMap)
{
	_dataSetTypes = theMap;
}

void ColumnEncoder::collectExtraEncodingsFromMetaJson(const Json::Value & json, std::vector<std::string> & namesCollected) const
{
	switch(json.type())
	{
	case Json::arrayValue:
		for(const Json::Value & option : json)
			collectExtraEncodingsFromMetaJson(option, namesCollected);
		return;

	case Json::objectValue:
		if(json.isMember("encodeThis"))
		{
			if(json["encodeThis"].isString())
				namesCollected.push_back(json["encodeThis"].asString());
			else if(json["encodeThis"].isArray())
				for(const Json::Value & enc : json["encodeThis"])
					namesCollected.push_back(enc.asString());
		}
		else
			for(const std::string & optionName : json.getMemberNames())
				collectExtraEncodingsFromMetaJson(json[optionName], namesCollected);
		return;

	default:
		return;
	}
}

std::string ColumnEncoder::removeColumnNamesFromRScript(const std::string & rCode, const std::vector<std::string> & colsToRemove)
{
	std::map<std::string, std::string> replaceBy;

	for(const std::string & col : colsToRemove)
		replaceBy[col] = "stop('column " + col + " was removed from this RScript')";

	return replaceColumnNamesInRScript(rCode, replaceBy);
}

std::string ColumnEncoder::replaceColumnNamesInRScript(const std::string & rCode, const std::map<std::string, std::string> & changedNames)
{
	//Ok the trick here is to reuse the encoding code, we will first encode the original names and then change the encodings to point back to the replaced names.
	ColumnEncoder tempEncoder(changedNames);

	return
		tempEncoder.replaceAll(
			tempEncoder.encodeRScript(
				rCode,
				tempEncoder._encodingMap,
				tempEncoder._originalNames
			),
			tempEncoder._decodingMap,
			tempEncoder._encodedNames
		);
}

ColumnEncoder::colVec ColumnEncoder::columnNames()
{
	return _columnEncoder ? _columnEncoder->_originalNames : colVec();
}

ColumnEncoder::colVec ColumnEncoder::columnNamesEncoded()
{
	return _columnEncoder ? _columnEncoder->_encodedNames : colVec();
}

void ColumnEncoder::_convertPreloadingDataOption(Json::Value & options, const std::string& optionName, colsPlusTypes& colTypes)
{
	Json::Value		newOption	=	Json::arrayValue,
					typeList	= options[optionName]["types"],
					valueList	= options[optionName]["value"];
	std::string		optionKey	= options[optionName].isMember("optionKey") ? options[optionName]["optionKey"].asString() : "";

	bool useSingleVal = false;

	if(valueList.isString())
	{
		std::string val = valueList.asString();
		valueList = Json::arrayValue;
		valueList.append(val);

		useSingleVal = true; //Otherwise we break things like "splitBy" it seems
	}
	if(typeList.isString())
	{
		std::string type = typeList.asString();
		typeList = Json::arrayValue;
		typeList.append(type);
	}

	// The valueList can be either;
	// . a list of strings, if it is a list of variables without interaction
	// . a list of array of strings if it is a list of variables with interaction
	// . a list of objects, if the value contains not only the variables but also some extra control values (rowComponent).
	//   In this case, is has an optionKey that gives where the variable names are.
	//	 Also here, there can be interaction, so the optionKey can give either a list of strings or a list of array of strings.

	for(int i=0; i<valueList.size(); i++)
	{
		Json::Value jsonType		= typeList.size() > i ? typeList[i] : Json::nullValue,
					jsonValueOrg	= valueList[i],
					jsonValue		= optionKey.empty() ? jsonValueOrg : jsonValueOrg[optionKey];

		if (jsonValue.isString())
		{
			std::string type = (jsonType.isString() ? jsonType.asString() : (jsonType.isArray() && jsonType.size() > 0 && jsonType[0].isString()) ? jsonType[0].asString() : "");
			bool hasType = type != "unknown" && columnTypeValidName(type);

			std::string columnName = jsonValue.asString();
			if(!hasType && columnName != "" && _columnEncoder->_dataSetTypes.count(columnName))
			{
				type = columnTypeToString(_columnEncoder->_dataSetTypes.at(columnName));
				hasType = type != "unknown";
			}

			std::string columnNameWithType = columnName.empty() ? "" : (columnName + (hasType ? "." + type : ""));

			if (optionKey.empty())
				newOption.append(columnNameWithType);
			else
			{
				// Reuse original jsonValue in order to get the other members of the object
				jsonValueOrg[optionKey] = columnNameWithType;
				newOption.append(jsonValueOrg);
			}

			if (!columnNameWithType.empty() && hasType)
				colTypes.insert(std::make_pair(columnNameWithType, columnTypeFromString(type)));
		}
		else if (jsonValue.isArray())
		{
			// Value with interaction: there are several column names.
			Json::Value newColumnNames(Json::arrayValue);
			int colNr = 0;
			for (const Json::Value& jsonColumnName : jsonValue)
			{
				colNr++;
				std::string type = jsonType.isString() ? jsonType.asString() : (jsonType.isArray() && jsonType.size() >= colNr && jsonType[colNr-1].isString() ? jsonType[colNr-1].asString() : "");
				bool hasType = type != "unknown" && columnTypeValidName(type);
				std::string columnName = jsonColumnName.asString();

				if(!hasType && columnName != "" && _columnEncoder->_dataSetTypes.count(columnName))
				{
					type = columnTypeToString(_columnEncoder->_dataSetTypes.at(columnName));
					hasType = type != "unknown";
				}

				std::string columnNameWithType = columnName.empty() ? "" : (columnName + (hasType ? "." + type : ""));
				newColumnNames.append(columnNameWithType);

				if (!columnNameWithType.empty() && hasType)
					colTypes.insert(std::make_pair(columnNameWithType, columnTypeFromString(type)));
			}
			if (optionKey.empty())
				newOption.append(newColumnNames);
			else
			{
				jsonValueOrg[optionKey] = newColumnNames;
				newOption.append(jsonValueOrg);
			}
		}
		else
			newOption.append(jsonValueOrg);
	}

	options[optionName + ".types"] = options[optionName]["types"];
	options[optionName] = !useSingleVal ? newOption : newOption[0];
}

void ColumnEncoder::_addTypeToColumnNamesInOptionsRecursively(Json::Value & options, bool preloadingData, colsPlusTypes& colTypes)
{
	if (options.isObject())
	{
		for (const std::string& optionName : options.getMemberNames())
		{
			if (options[optionName].isObject() && options[optionName].isMember("value") && options[optionName].isMember("types"))
			{
				if(preloadingData)
					_convertPreloadingDataOption(options, optionName, colTypes);
				else //make sure "optionnam	e".types is available for analyses incapable of preloadingData, this should be considered deprecated
				{
					options[optionName + ".types"] = options[optionName]["types"];
					options[optionName] = options[optionName]["value"];
				}
			}
			else
				_addTypeToColumnNamesInOptionsRecursively(options[optionName], preloadingData, colTypes);
		}
	}
	else if (options.isArray())
	{
		for (Json::Value& oneOption : options)
			_addTypeToColumnNamesInOptionsRecursively(oneOption, preloadingData, colTypes);
	}
}

ColumnEncoder::colsPlusTypes ColumnEncoder::encodeColumnNamesinOptions(Json::Value & options, bool preloadingData)
{
	colsPlusTypes getTheseCols;

	_addTypeToColumnNamesInOptionsRecursively(options, preloadingData, getTheseCols);

	//LOGGER << "Options before encoding: " << options.toStyledString() << std::endl;

	_encodeColumnNamesinOptions(options, options[".meta"]);

	//LOGGER << "Options after encoding: " << options.toStyledString() << std::endl;
	return getTheseCols;
}

void ColumnEncoder::_encodeColumnNamesinOptions(Json::Value & options, Json::Value & meta)
{
	if(meta.isNull())
		return;
	
	bool	encodePlease	= meta.isObject() && meta.get("shouldEncode",	false).asBool(),
			isRCode			= meta.isObject() && meta.get("rCode",			false).asBool();

	switch(options.type())
	{
	case Json::arrayValue:
		if(encodePlease)
			columnEncoder()->encodeJson(options, false, true); //If we already think we have columnNames just change it all
		
		else if(meta.type() == Json::arrayValue)
			for(int i=0; i<options.size() && i < meta.size(); i++)
				_encodeColumnNamesinOptions(options[i], meta[i]);

		else if(isRCode)
		{
			for(int i=0; i<options.size(); i++)
				if(options[i].isString())
					options[i] = columnEncoder()->encodeRScript(options[i].asString());
		}
		else if(meta.type() == Json::objectValue) // The option is an array, and the meta is an object: each option element in the array must be encoded with the same meta
			for(int i=0; i<options.size(); i++)
				_encodeColumnNamesinOptions(options[i], meta);


		return;

	case Json::objectValue:
		for(const std::string & memberName : options.getMemberNames())
			if(memberName != ".meta" && meta.isMember(memberName))
				_encodeColumnNamesinOptions(options[memberName], meta[memberName]);
		
			else if(isRCode && options[memberName].isString())
				options[memberName] = columnEncoder()->encodeRScript(options[memberName].asString());
		
			else if(encodePlease)
				columnEncoder()->encodeJson(options, false, true); //If we already think we have columnNames just change it all I guess?
		
		return;

	case Json::stringValue:
			
			if(isRCode)				options = columnEncoder()->encodeRScript(options.asString());
			else if(encodePlease)	options = columnEncoder()->encodeAll(options.asString());
			
		return;

	default:
		return;
	}
}
