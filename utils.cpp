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


#include "utils.h"

#ifdef _WIN32
#include <windows.h>
#include <fileapi.h>
#include <winternl.h>
#else
#include <sys/stat.h>
#include <utime.h>
#endif

#include <cmath>
#include "utilenums.h"
#include <iomanip>
#include <chrono>

#ifdef BUILDING_JASP
#include "log.h"
#define LOGGER Log::log()
#else
#include <Rcpp.h>
#define LOGGER Rcpp::Rcout
#endif

using namespace std;

Utils::FileType Utils::getTypeFromFileName(const std::string &path)
{

	size_t lastPoint = path.find_last_of('.');
	FileType filetype =  lastPoint == string::npos ?  FileType::empty : FileType::unknown;

	if (lastPoint != string::npos && (lastPoint + 1) < path.length())
	{
		std::string suffix = path.substr(lastPoint + 1);
		for (int i = 0; i < int(FileType::empty); i++)
		{
			FileType it = static_cast<FileType>(i);
			if (suffix == FileTypeBaseToString(it))
			{
				filetype = it;
				break;
			}
		}
	}


	return filetype;
}

const string &Utils::currentDateTime()
{
	static std::string currentDateTimeCache;

	auto now = std::chrono::system_clock::now();
	auto in_time_t = std::chrono::system_clock::to_time_t(now);

	std::stringstream ss;
	ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d %X");

	currentDateTimeCache = ss.str();

	return currentDateTimeCache;
}

int64_t Utils::currentMillis()
{
	return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

int64_t Utils::currentSeconds()
{
	return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

int64_t Utils::getFileModificationTime(const std::string &filename)
{
#ifdef _WIN32

	HANDLE file = CreateFileA(filename.c_str(), GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);

	if (file == INVALID_HANDLE_VALUE)
	{
		LOGGER << "Utils::getFileModificationTime(" << filename << ") failed to open the file!" << std::endl;
		return -1;
	}
	
	FILETIME modTime;

	bool success = GetFileTime(file, NULL, NULL, &modTime);
	CloseHandle(file);

	if (success)
	{
		LARGE_INTEGER li;
		ULONG         seconds;
		li.QuadPart = modTime.dwHighDateTime;
		li.QuadPart = (li.QuadPart << 32) | modTime.dwLowDateTime;
		RtlTimeToSecondsSince1970(&li, &seconds);

		return seconds;
	}
	else
	{
		return -1;
	}
#elif __APPLE__

	struct stat attrib;
	stat(filename.c_str(), &attrib);
	time_t modificationTime = attrib.st_mtimespec.tv_sec;

	return modificationTime;

#else
	struct stat attrib;
	stat(filename.c_str(), &attrib);
	time_t modificationTime = attrib.st_mtim.tv_sec;

	return modificationTime;
#endif
}

int64_t Utils::getFileSize(const string &filename)
{
	std::error_code ec;
	std::filesystem::path path;

	path = filename;


	uintmax_t fileSize = std::filesystem::file_size(path, ec);

	if (!ec)
		return fileSize;
	return -1;
}

void Utils::touch(const string &filename)
{
#ifdef _WIN32

	HANDLE file = CreateFileA(filename.c_str(), FILE_WRITE_ATTRIBUTES, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);

	if (file == INVALID_HANDLE_VALUE)
	{
		LOGGER << "Utils::touch(" << filename << ") failed to open the file!" << std::endl;
		return;
	}

	FILETIME ft;
	SYSTEMTIME st;

	GetSystemTime(&st);
	SystemTimeToFileTime(&st, &ft);
	SetFileTime(file, NULL, NULL, &ft);

	CloseHandle(file);

#else
	struct utimbuf newTime;

	time_t newTimeT;
	time(&newTimeT);

	newTime.actime = newTimeT;
	newTime.modtime = newTimeT;

	int error = utime(filename.c_str(), &newTime);
	
	static auto errorLog = [filename](const std::string & error)
	{
		LOGGER << "Utils::touch(" << filename << ") failed with error: " << error << std::endl;
	};
	
	switch(error)
	{
	case EACCES:		
		errorLog("Search permission is denied by a component of the path prefix; or the times argument is a null pointer and the effective user ID of the process does not match the owner of the file, the process does not have write permission for the file, and the process does not have appropriate privileges."); 
		break;
		
	case ELOOP:		
		errorLog("A loop exists in symbolic links encountered during resolution of the path argument.\n"
				 "Or: more than {SYMLOOP_MAX} symbolic links were encountered during resolution of the path argument.");
		break;
	
	case ENAMETOOLONG:		
		errorLog("The length of the path argument exceeds {PATH_MAX} or a pathname component is longer than {NAME_MAX}.\n"
				"Or: as a result of encountering a symbolic link in resolution of the path argument, the length of the substituted pathname string exceeded {PATH_MAX}."); 
		break;
		
	case ENOENT:		
			errorLog("A component of path does not name an existing file or path is an empty string."); 
		break;
			
	case ENOTDIR:		
			errorLog("A component of the path prefix is not a directory."); 
		break;
			
	case EPERM:		
			errorLog("The times argument is not a null pointer and the calling process' effective user ID does not match the owner of the file and the calling process does not have the appropriate privileges."); 
		break;
			
	case EROFS:		
			errorLog("The file system containing the file is read-only."); 
		break;
			
	default:
		if(error > 0)
			errorLog("Some unknown error (" + std::to_string(error) + ") occurred..."); 
		break;
	}
#endif
}

bool Utils::renameOverwrite(const string &oldName, const string &newName)
{
	std::filesystem::path o = osPath(oldName);
	std::filesystem::path n = osPath(newName);
	std::error_code ec;

#ifdef _WIN32
	std::error_code er;
	if (std::filesystem::exists(n, er))
	{
		std::filesystem::file_status s = std::filesystem::status(n);
		
		bool readOnly = (s.permissions() & std::filesystem::perms::owner_write) == std::filesystem::perms::none;
		if (readOnly)
			std::filesystem::permissions(n, std::filesystem::perms::owner_write);
	}
#endif

	std::filesystem::rename(o, n, ec);

	return !ec;
}

bool Utils::removeFile(const string &path)
{
	std::filesystem::path p = osPath(path);
	std::error_code ec;

	std::filesystem::remove(p, ec);

	return !ec;
}

std::filesystem::path Utils::osPath(const string &path)
{
	return std::filesystem::path(path);
}

string Utils::osPath(const std::filesystem::path &path)
{
	return path.generic_string();
}

void Utils::remove(vector<string> &target, const vector<string> &toRemove)
{
	for (const string &remove : toRemove)
		target.erase(std::remove_if(target.begin(), target.end(), [&remove](const string& str){return (str == remove);}), target.end());
}

void Utils::sleep(int ms)
{

#ifdef _WIN32
	Sleep(DWORD(ms));
#else
	struct timespec ts = { ms / 1000, (ms % 1000) * 1000 * 1000 };
	nanosleep(&ts, NULL);
#endif
}

bool Utils::isEqual(const double a, const double b)
{
	if (isnan(a) || isnan(b)) return (isnan(a) && isnan(b));

	return (fabs(a - b) <= ( (fabs(a) < fabs(b) ? fabs(b) : fabs(a)) * std::numeric_limits<double>::epsilon()));
}

bool Utils::isEqual(const float a, const float b)
{
	if (isnan(a) || isnan(b)) return (isnan(a) && isnan(b));

	return fabs(a - b) <= ( (fabs(a) < fabs(b) ? fabs(b) : fabs(a)) * std::numeric_limits<float>::epsilon());
}

#ifdef _WIN32
std::wstring Utils::getShortPathWin(const std::wstring & longPath) 
{
	//See: https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getshortpathnamew
	long     length = 0;
	WCHAR*   buffer = NULL;

// First obtain the size needed by passing NULL and 0.

	length = GetShortPathNameW(longPath.c_str(), NULL, 0);
	if (length == 0) 
		return longPath;

// Dynamically allocate the correct size 
// (terminating null char was included in length)

	buffer = new WCHAR[length];

// Now simply call again using same long path.

	length = GetShortPathNameW(longPath.c_str(), buffer, length);
	if (length == 0)
	{
		delete[] buffer;
		return longPath;
	}

	std::wstring shortPath(buffer, length);
	
	delete [] buffer;
	
	return shortPath;
}

string Utils::wstringToString(const std::wstring & wstr)
{
	std::string str;

	//get size of buffer we need
	int requiredSize = WideCharToMultiByte(CP_UTF8, 0, wstr.data(), -1, NULL, 0, NULL, NULL );
	str.resize(requiredSize);

	//convert it
	WideCharToMultiByte(CP_UTF8, 0, wstr.data(), -1, str.data(), str.size(), NULL, NULL );
	str.resize(requiredSize-1);//drop /nul

	return str;

}

wstring Utils::stringToWString(const std::string &str)
{
	std::wstring wstr;

	//get size of buffer we need
	int requiredSize = MultiByteToWideChar(CP_UTF8, 0, str.data(), -1, NULL, 0);
	wstr.resize(requiredSize);

	//convert it
	MultiByteToWideChar(CP_UTF8, 0, str.data(), -1, wstr.data(), wstr.size());
	wstr.resize(requiredSize-1);//drop /nul

	return wstr;
}
#endif
