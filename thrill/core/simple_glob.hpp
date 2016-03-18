/*******************************************************************************
 * thrill/core/simple_glob.hpp
 *
 * A sane, simple, portable implementation of glob(). Used only on Windows.
 *
 * copied from SimpleOpt/SimpleGlob under MIT License
 * Copyright (c) 2006-2013, Brodie Thiesfield
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_SIMPLE_GLOB_HEADER
#define THRILL_CORE_SIMPLE_GLOB_HEADER

// ---------------------------------------------------------------------------
// Platform dependent implementations

// if we aren't on Windows and we have ICU available, then enable ICU
// by default. Define this to 0 to intentially disable it.
#ifndef SG_HAVE_ICU
# if !defined(_WIN32) && defined(USTRING_H)
#   define SG_HAVE_ICU 1
# else
#   define SG_HAVE_ICU 0
# endif
#endif

// don't include this in documentation as it isn't relevant
#ifndef DOXYGEN

// on Windows we want to use MBCS aware string functions and mimic the
// Unix glob functionality. On Unix we just use glob.
#ifdef _WIN32
# include <mbstring.h>
# include <windows.h>
# define sg_strchr          ::_mbschr
# define sg_strrchr         ::_mbsrchr
# define sg_strlen          ::_mbslen
# if __STDC_WANT_SECURE_LIB__
#  define sg_strcpy_s(a, n, b) ::_mbscpy_s(a, n, b)
# else
#  define sg_strcpy_s(a, n, b) ::_mbscpy(a, b)
# endif
# define sg_strcmp          ::_mbscmp
# define sg_strcasecmp      ::_mbsicmp
# define SOCHAR_T           unsigned char
#else
# include <climits>
# include <glob.h>      // NOLINT
# include <sys/stat.h>  // NOLINT
# include <sys/types.h> // NOLINT
# define MAX_PATH           PATH_MAX
# define sg_strchr          ::strchr
# define sg_strrchr         ::strrchr
# define sg_strlen          ::strlen
# define sg_strcpy_s(a, n, b) ::strcpy(a, b) // NOLINT
# define sg_strcmp          ::strcmp
# define sg_strcasecmp      ::strcasecmp
# define SOCHAR_T           char
#endif

#include <wchar.h>

#include <cstdlib>
#include <cstring>

// use assertions to test the input data
#ifdef _DEBUG
# ifdef _MSC_VER
#  include <crtdbg.h>
#  define SG_ASSERT(b)    _ASSERTE(b)
# else
#  include <cassert>
#  define SG_ASSERT(b)    assert(b)
# endif
#else
# define SG_ASSERT(b)
#endif

namespace thrill {
namespace core {
namespace glob_local {

/*! \file simple_glob.hpp

    \version 3.6

    \brief A cross-platform file globbing library providing the ability to
    expand wildcards in command-line arguments to a list of all matching
    files. It is designed explicitly to be portable to any platform and has
    been tested on Windows and Linux. See CSimpleGlobTempl for the class
    definition.

    \section features FEATURES
    -   MIT Licence allows free use in all software (including GPL and
        commercial)
    -   multi-platform (Windows 95/98/ME/NT/2K/XP, Linux, Unix)
    -   supports most of the standard linux glob() options
    -   recognition of a forward paths as equivalent to a backward slash
        on Windows. e.g. "c:/path/foo*" is equivalent to "c:\path\foo*".
    -   implemented with only a single C++ header file
    -   char, wchar_t and Windows TCHAR in the same program
    -   complete working examples included
    -   compiles cleanly at warning level 4 (Windows/VC.NET 2003),
        warning level 3 (Windows/VC6) and -Wall (Linux/gcc)

    \section usage USAGE
    The SimpleGlob class is used by following these steps:
    <ol>
    <li> Include the SimpleGlob.h header file

        <pre>
        \#include "SimpleGlob.h"
        </pre>

   <li> Instantiate a CSimpleGlob object supplying the appropriate flags.

        <pre>
        CSimpleGlob glob(FLAGS);
        </pre>

   <li> Add all file specifications to the glob class.

        <pre>
        glob.Add("file*");
        glob.Add(argc, argv);
        </pre>

   <li> Process all files with File(), Files() and FileCount()

        <pre>
        for (int n = 0; n < glob.FileCount(); ++n) {
            ProcessFile(glob.File(n));
        }
        </pre>

    </ol>

    \section licence MIT LICENCE
<pre>
    The licence text below is the boilerplate "MIT Licence" used from:
    http://www.opensource.org/licenses/mit-license.php

    Copyright (c) 2006-2013, Brodie Thiesfield

    Permission is hereby granted, free of charge, to any person obtaining a
    copy of this software and associated documentation files (the "Software"),
    to deal in the Software without restriction, including without limitation
    the rights to use, copy, modify, merge, publish, distribute, sublicense,
    and/or sell copies of the Software, and to permit persons to whom the
    Software is furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included
    in all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
    OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
    MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
    IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
    CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
    TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
    SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
</pre>
*/

/*! \brief The operation of SimpleGlob is fine-tuned via the use of a
    combination of the following flags.

    The flags may be passed at initialization of the class and used for every
    filespec added, or alternatively they may optionally be specified in the
    call to Add() and be different for each filespec.

    \param SG_GLOB_ERR
        Return upon read error (e.g. directory does not have read permission)

    \param SG_GLOB_MARK
        Append a slash (backslash in Windows) to every path which corresponds
        to a directory

    \param SG_GLOB_NOSORT
        By default, files are returned in sorted into string order. With this
        flag, no sorting is done. This is not compatible with
        SG_GLOB_FULLSORT.

    \param SG_GLOB_FULLSORT
        By default, files are sorted in groups belonging to each filespec that
        was added. For example if the filespec "b*" was added before the
        filespec "a*" then the argv array will contain all b* files sorted in
        order, followed by all a* files sorted in order. If this flag is
        specified, the entire array will be sorted ignoring the filespec
        groups.

    \param SG_GLOB_NOCHECK
        If the pattern doesn't match anything, return the original pattern.

    \param SG_GLOB_TILDE
        Tilde expansion is carried out (on Unix platforms)

    \param SG_GLOB_ONLYDIR
        Return only directories which match (not compatible with
        SG_GLOB_ONLYFILE)

    \param SG_GLOB_ONLYFILE
        Return only files which match (not compatible with SG_GLOB_ONLYDIR)

    \param SG_GLOB_NODOT
        Do not return the "." or ".." special directories.
 */
enum SG_Flags {
    SG_GLOB_ERR = 1 << 0,
    SG_GLOB_MARK = 1 << 1,
    SG_GLOB_NOSORT = 1 << 2,
    SG_GLOB_NOCHECK = 1 << 3,
    SG_GLOB_TILDE = 1 << 4,
    SG_GLOB_ONLYDIR = 1 << 5,
    SG_GLOB_ONLYFILE = 1 << 6,
    SG_GLOB_NODOT = 1 << 7,
    SG_GLOB_FULLSORT = 1 << 8
};

/*! \brief Error return codes */
enum SG_Error {
    SG_SUCCESS = 0,
    SG_ERR_NOMATCH = 1,
    SG_ERR_MEMORY = -1,
    SG_ERR_FAILURE = -2
};

/*! \brief String manipulation functions. */
class SimpleGlobUtil
{
public:
    static const char * strchr(const char* s, char c) {
        return reinterpret_cast<const char*>(
            sg_strchr(reinterpret_cast<const SOCHAR_T*>(s), c));
    }
    static const wchar_t * strchr(const wchar_t* s, wchar_t c) {
        return ::wcschr(s, c);
    }
#if SG_HAVE_ICU
    static const UChar * strchr(const UChar* s, UChar c) {
        return ::u_strchr(s, c);
    }
#endif

    static const char * strrchr(const char* s, char c) {
        return reinterpret_cast<const char*>(
            sg_strrchr(reinterpret_cast<const SOCHAR_T*>(s), c));
    }
    static const wchar_t * strrchr(const wchar_t* s, wchar_t c) {
        return ::wcsrchr(s, c);
    }
#if SG_HAVE_ICU
    static const UChar * strrchr(const UChar* s, UChar c) {
        return ::u_strrchr(s, c);
    }
#endif

    // Note: char strlen returns number of bytes, not characters
    static size_t strlen(const char* s) { return ::strlen(s); }
    static size_t strlen(const wchar_t* s) { return ::wcslen(s); }
#if SG_HAVE_ICU
    static size_t strlen(const UChar* s) { return ::u_strlen(s); }
#endif

    static void strcpy_s(char* dst, size_t n, const char* src) {
        (void)n;
        sg_strcpy_s(reinterpret_cast<SOCHAR_T*>(dst), n,
                    reinterpret_cast<const SOCHAR_T*>(src));
    }
    static void strcpy_s(wchar_t* dst, size_t n, const wchar_t* src) {
# if __STDC_WANT_SECURE_LIB__
        ::wcscpy_s(dst, n, src);
#else
        (void)n;
        ::wcscpy(dst, src);
#endif
    }
#if SG_HAVE_ICU
    static void strcpy_s(UChar* dst, size_t n, const UChar* src) {
        ::u_strncpy(dst, src, n);
    }
#endif

    static int strcmp(const char* s1, const char* s2) {
        return sg_strcmp((const SOCHAR_T*)s1, (const SOCHAR_T*)s2);
    }
    static int strcmp(const wchar_t* s1, const wchar_t* s2) {
        return ::wcscmp(s1, s2);
    }
#if SG_HAVE_ICU
    static int strcmp(const UChar* s1, const UChar* s2) {
        return ::u_strcmp(s1, s2);
    }
#endif

    static int strcasecmp(const char* s1, const char* s2) {
        return sg_strcasecmp((const SOCHAR_T*)s1, (const SOCHAR_T*)s2);
    }
#if _WIN32
    static int strcasecmp(const wchar_t* s1, const wchar_t* s2) {
        return ::_wcsicmp(s1, s2);
    }
#endif          // _WIN32
#if SG_HAVE_ICU
    static int strcasecmp(const UChar* s1, const UChar* s2) {
        return u_strcasecmp(s1, s2, 0);
    }
#endif
};

enum SG_FileType {
    SG_FILETYPE_INVALID,
    SG_FILETYPE_FILE,
    SG_FILETYPE_DIR
};

#ifdef _WIN32

#ifndef INVALID_FILE_ATTRIBUTES
# define INVALID_FILE_ATTRIBUTES    ((DWORD)-1)
#endif

#define SG_PATH_CHAR    '\\'

/*! \brief Windows glob implementation. */
template <typename SOCHAR>
class SimpleGlobBase
{
public:
    SimpleGlobBase() : m_hFind(INVALID_HANDLE_VALUE) { }

    int FindFirstFileS(const char* a_pszFileSpec, unsigned int) {
        m_hFind = FindFirstFileA(a_pszFileSpec, &m_oFindDataA);
        if (m_hFind != INVALID_HANDLE_VALUE) {
            return SG_SUCCESS;
        }
        DWORD dwErr = GetLastError();
        if (dwErr == ERROR_FILE_NOT_FOUND) {
            return SG_ERR_NOMATCH;
        }
        return SG_ERR_FAILURE;
    }
    int FindFirstFileS(const wchar_t* a_pszFileSpec, unsigned int) {
        m_hFind = FindFirstFileW(a_pszFileSpec, &m_oFindDataW);
        if (m_hFind != INVALID_HANDLE_VALUE) {
            return SG_SUCCESS;
        }
        DWORD dwErr = GetLastError();
        if (dwErr == ERROR_FILE_NOT_FOUND) {
            return SG_ERR_NOMATCH;
        }
        return SG_ERR_FAILURE;
    }

    bool FindNextFileS(char) {    // NOLINT
        return FindNextFileA(m_hFind, &m_oFindDataA) != FALSE;
    }
    bool FindNextFileS(wchar_t) { // NOLINT
        return FindNextFileW(m_hFind, &m_oFindDataW) != FALSE;
    }

    void FindDone() {
        FindClose(m_hFind);
    }

    const char * GetFileNameS(char) const {       // NOLINT
        return m_oFindDataA.cFileName;
    }
    const wchar_t * GetFileNameS(wchar_t) const { // NOLINT
        return m_oFindDataW.cFileName;
    }

    bool IsDirS(char) const {                     // NOLINT
        return this->GetFileTypeS(m_oFindDataA.dwFileAttributes) == SG_FILETYPE_DIR;
    }
    bool IsDirS(wchar_t) const {                  // NOLINT
        return this->GetFileTypeS(m_oFindDataW.dwFileAttributes) == SG_FILETYPE_DIR;
    }

    SG_FileType GetFileTypeS(const char* a_pszPath) {
        return this->GetFileTypeS(GetFileAttributesA(a_pszPath));
    }
    SG_FileType GetFileTypeS(const wchar_t* a_pszPath) {
        return this->GetFileTypeS(GetFileAttributesW(a_pszPath));
    }
    SG_FileType GetFileTypeS(DWORD a_dwAttribs) const {
        if (a_dwAttribs == INVALID_FILE_ATTRIBUTES) {
            return SG_FILETYPE_INVALID;
        }
        if (a_dwAttribs & FILE_ATTRIBUTE_DIRECTORY) {
            return SG_FILETYPE_DIR;
        }
        return SG_FILETYPE_FILE;
    }

private:
    HANDLE m_hFind;
    WIN32_FIND_DATAA m_oFindDataA;
    WIN32_FIND_DATAW m_oFindDataW;
};

#else       // !_WIN32

#define SG_PATH_CHAR    '/'

/*! \brief Unix glob implementation. */
template <typename SOCHAR>
class SimpleGlobBase
{
public:
    SimpleGlobBase() {
        memset(&glob_, 0, sizeof(glob_));
        ui_curr_ = (size_t)-1;
    }

    ~SimpleGlobBase() {
        globfree(&glob_);
    }

    void FilePrep() {
        b_isdir_ = false;
        size_t len = strlen(glob_.gl_pathv[ui_curr_]);
        if (glob_.gl_pathv[ui_curr_][len - 1] == '/') {
            b_isdir_ = true;
            glob_.gl_pathv[ui_curr_][len - 1] = 0;
        }
    }

    int FindFirstFileS(const char* a_pszFileSpec, unsigned int a_uiFlags) {
        int nflags = GLOB_MARK | GLOB_NOSORT;
        if (a_uiFlags & SG_GLOB_ERR) nflags |= GLOB_ERR;
        if (a_uiFlags & SG_GLOB_TILDE) nflags |= GLOB_TILDE;
        int rc = glob(a_pszFileSpec, nflags, nullptr, &glob_);
        if (rc == GLOB_NOSPACE) return SG_ERR_MEMORY;
        if (rc == GLOB_ABORTED) return SG_ERR_FAILURE;
        if (rc == GLOB_NOMATCH) return SG_ERR_NOMATCH;
        ui_curr_ = 0;
        FilePrep();
        return SG_SUCCESS;
    }

#if SG_HAVE_ICU
    int FindFirstFileS(const UChar* a_pszFileSpec, unsigned int a_uiFlags) {
        char buf[PATH_MAX] = { 0 };
        UErrorCode status = U_ZERO_ERROR;
        u_strToUTF8(buf, sizeof(buf), nullptr, a_pszFileSpec, -1, &status);
        if (U_FAILURE(status)) return SG_ERR_FAILURE;
        return this->FindFirstFileS(buf, a_uiFlags);
    }
#endif

    bool FindNextFileS(char) { // NOLINT
        SG_ASSERT(ui_curr_ != (size_t)-1);
        if (++ui_curr_ >= glob_.gl_pathc) {
            return false;
        }
        FilePrep();
        return true;
    }

#if SG_HAVE_ICU
    bool FindNextFileS(UChar) { // NOLINT
        return this->FindNextFileS(static_cast<char>(0));
    }
#endif

    void FindDone() {
        globfree(&glob_);
        memset(&glob_, 0, sizeof(glob_));
        ui_curr_ = (size_t)-1;
    }

    const char * GetFileNameS(char) const { // NOLINT
        SG_ASSERT(ui_curr_ != (size_t)-1);
        return glob_.gl_pathv[ui_curr_];
    }

#if SG_HAVE_ICU
    const UChar * GetFileNameS(UChar) const { // NOLINT
        const char* pszFile = this->GetFileNameS(static_cast<char>(0));
        if (!pszFile) return nullptr;
        UErrorCode status = U_ZERO_ERROR;
        memset(m_szBuf, 0, sizeof(m_szBuf));
        u_strFromUTF8(m_szBuf, PATH_MAX, nullptr, pszFile, -1, &status);
        if (U_FAILURE(status)) return nullptr;
        return m_szBuf;
    }
#endif

    bool IsDirS(char) const { // NOLINT
        SG_ASSERT(ui_curr_ != (size_t)-1);
        return b_isdir_;
    }

#if SG_HAVE_ICU
    bool IsDirS(UChar) const { // NOLINT
        return this->IsDirS(static_cast<char>(0));
    }
#endif

    SG_FileType GetFileTypeS(const char* a_pszPath) const {
        struct stat sb;
        if (0 != stat(a_pszPath, &sb)) {
            return SG_FILETYPE_INVALID;
        }
        if (S_ISDIR(sb.st_mode)) {
            return SG_FILETYPE_DIR;
        }
        if (S_ISREG(sb.st_mode)) {
            return SG_FILETYPE_FILE;
        }
        return SG_FILETYPE_INVALID;
    }

#if SG_HAVE_ICU
    SG_FileType GetFileTypeS(const UChar* a_pszPath) const {
        char buf[PATH_MAX] = { 0 };
        UErrorCode status = U_ZERO_ERROR;
        u_strToUTF8(buf, sizeof(buf), nullptr, a_pszPath, -1, &status);
        if (U_FAILURE(status)) return SG_FILETYPE_INVALID;
        return this->GetFileTypeS(buf);
    }
#endif

private:
    glob_t glob_;
    size_t ui_curr_;
    bool b_isdir_;
#if SG_HAVE_ICU
    mutable UChar m_szBuf[PATH_MAX];
#endif
};

#endif // _WIN32

#endif // DOXYGEN

// ---------------------------------------------------------------------------
//                              MAIN TEMPLATE CLASS
// ---------------------------------------------------------------------------

/*! \brief Implementation of the SimpleGlob class */
template <typename SOCHAR>
class CSimpleGlobTempl : private SimpleGlobBase<SOCHAR>
{
public:
    /*! \brief Initialize the class.

        \param a_uiFlags            Combination of SG_GLOB flags.
        \param a_nReservedSlots     Number of slots in the argv array that
            should be reserved. In the returned array these slots
            argv[0] ... argv[a_nReservedSlots-1] will be left empty for
            the caller to fill in.
     */
    explicit CSimpleGlobTempl(unsigned int a_uiFlags = 0, int a_nReservedSlots = 0);

    /*! \brief Deallocate all memory buffers. */
    ~CSimpleGlobTempl();

    /*! \brief Initialize (or re-initialize) the class in preparation for
        adding new filespecs.

        All existing files are cleared. Note that allocated memory is only
        deallocated at object destruction.

        \param a_uiFlags            Combination of SG_GLOB flags.
        \param a_nReservedSlots     Number of slots in the argv array that
            should be reserved. In the returned array these slots
            argv[0] ... argv[a_nReservedSlots-1] will be left empty for
            the caller to fill in.
     */
    int Init(unsigned int a_uiFlags = 0, int a_nReservedSlots = 0);

    /*! \brief Add a new filespec to the glob.

        The filesystem will be immediately scanned for all matching files and
        directories and they will be added to the glob.

        \param a_pszFileSpec    Filespec to add to the glob.

        \return SG_SUCCESS      Matching files were added to the glob.
        \return SG_ERR_NOMATCH  Nothing matched the pattern. To ignore this
                                error compare return value to >= SG_SUCCESS.
        \return SG_ERR_MEMORY   Out of memory failure.
        \return SG_ERR_FAILURE  General failure.
     */
    int Add(const SOCHAR* a_pszFileSpec);

    /*! \brief Add an array of filespec to the glob.

        The filesystem will be immediately scanned for all matching files and
        directories in each filespec and they will be added to the glob.

        \param a_nCount         Number of filespec in the array.
        \param a_rgpszFileSpec  Array of filespec to add to the glob.

        \return SG_SUCCESS      Matching files were added to the glob.
        \return SG_ERR_NOMATCH  Nothing matched the pattern. To ignore this
                                error compare return value to >= SG_SUCCESS.
        \return SG_ERR_MEMORY   Out of memory failure.
        \return SG_ERR_FAILURE  General failure.
     */
    int Add(int a_nCount, const SOCHAR* const* a_rgpszFileSpec);

    /*! \brief Return the number of files in the argv array.
     */
    inline int FileCount() const { return m_nArgsLen; }

    /*! \brief Return the full argv array. */
    inline SOCHAR ** Files() {
        SetArgvArrayType(POINTERS);
        return m_rgpArgs;
    }

    /*! \brief Return the a single file. */
    inline SOCHAR * File(int n) {
        SG_ASSERT(n >= 0 && n < m_nArgsLen);
        return Files()[n];
    }

private:
    CSimpleGlobTempl(const CSimpleGlobTempl&);              // disabled
    CSimpleGlobTempl& operator = (const CSimpleGlobTempl&); // disabled

    /*! \brief The argv array has it's members stored as either an offset into
        the string buffer, or as pointers to their string in the buffer. The
        offsets are used because if the string buffer is dynamically resized,
        all pointers into that buffer would become invalid.
     */
    enum ARG_ARRAY_TYPE { OFFSETS, POINTERS };

    /*! \brief Change the type of data stored in the argv array. */
    void SetArgvArrayType(ARG_ARRAY_TYPE a_nNewType);

    /*! \brief Add a filename to the array if it passes all requirements. */
    int AppendName(const SOCHAR* a_pszFileName, bool a_bIsDir);

    /*! \brief Grow the argv array to the required size. */
    bool GrowArgvArray(int a_nNewLen);

    /*! \brief Grow the string buffer to the required size. */
    bool GrowStringBuffer(size_t a_uiMinSize);

    /*! \brief Compare two (possible nullptr) strings */
    static int fileSortCompare(const void* a1, const void* a2);

private:
    unsigned int m_uiFlags;
    ARG_ARRAY_TYPE m_nArgArrayType;         //!< argv is indexes or pointers
    SOCHAR** m_rgpArgs;                     //!< argv
    int m_nReservedSlots;                   //!< # client slots in argv array
    int m_nArgsSize;                        //!< allocated size of array
    int m_nArgsLen;                         //!< used length
    SOCHAR* m_pBuffer;                      //!< argv string buffer
    size_t m_uiBufferSize;                  //!< allocated size of buffer
    size_t m_uiBufferLen;                   //!< used length of buffer
    SOCHAR m_szPathPrefix[MAX_PATH];        //!< wildcard path prefix
};

// ---------------------------------------------------------------------------
//                                  IMPLEMENTATION
// ---------------------------------------------------------------------------

template <typename SOCHAR>
CSimpleGlobTempl<SOCHAR>::CSimpleGlobTempl(
    unsigned int a_uiFlags,
    int a_nReservedSlots
    ) {
    m_rgpArgs = nullptr;
    m_nArgsSize = 0;
    m_pBuffer = nullptr;
    m_uiBufferSize = 0;

    Init(a_uiFlags, a_nReservedSlots);
}

template <typename SOCHAR>
CSimpleGlobTempl<SOCHAR>::~CSimpleGlobTempl() {
    if (m_rgpArgs) free(m_rgpArgs);
    if (m_pBuffer) free(m_pBuffer);
}

template <typename SOCHAR>
int
CSimpleGlobTempl<SOCHAR>::Init(
    unsigned int a_uiFlags,
    int a_nReservedSlots
    ) {
    m_nArgArrayType = POINTERS;
    m_uiFlags = a_uiFlags;
    m_nArgsLen = a_nReservedSlots;
    m_nReservedSlots = a_nReservedSlots;
    m_uiBufferLen = 0;

    if (m_nReservedSlots > 0) {
        if (!GrowArgvArray(m_nReservedSlots)) {
            return SG_ERR_MEMORY;
        }
        for (int n = 0; n < m_nReservedSlots; ++n) {
            m_rgpArgs[n] = nullptr;
        }
    }

    return SG_SUCCESS;
}

template <typename SOCHAR>
int
CSimpleGlobTempl<SOCHAR>::Add(
    const SOCHAR* a_pszFileSpec
    ) {
#ifdef _WIN32
    // Windows FindFirst/FindNext recognizes forward slash as the same as
    // backward slash and follows the directories. We need to do the same
    // when calculating the prefix and when we have no wildcards.
    SOCHAR szFileSpec[MAX_PATH];
    SimpleGlobUtil::strcpy_s(szFileSpec, MAX_PATH, a_pszFileSpec);
    const SOCHAR* pszPath = SimpleGlobUtil::strchr(szFileSpec, '/');
    while (pszPath) {
        szFileSpec[pszPath - szFileSpec] = SG_PATH_CHAR;
        pszPath = SimpleGlobUtil::strchr(pszPath + 1, '/');
    }
    a_pszFileSpec = szFileSpec;
#endif

    // if this doesn't contain wildcards then we can just add it directly
    m_szPathPrefix[0] = 0;
    if (!SimpleGlobUtil::strchr(a_pszFileSpec, '*') &&
        !SimpleGlobUtil::strchr(a_pszFileSpec, '?'))
    {
        SG_FileType nType = this->GetFileTypeS(a_pszFileSpec);
        if (nType == SG_FILETYPE_INVALID) {
            if (m_uiFlags & SG_GLOB_NOCHECK) {
                return AppendName(a_pszFileSpec, false);
            }
            return SG_ERR_NOMATCH;
        }
        return AppendName(a_pszFileSpec, nType == SG_FILETYPE_DIR);
    }

#ifdef _WIN32
    // Windows doesn't return the directory with the filename, so we need to
    // extract the path from the search string ourselves and prefix it to the
    // filename we get back.
    const SOCHAR* pszFilename =
        SimpleGlobUtil::strrchr(a_pszFileSpec, SG_PATH_CHAR);
    if (pszFilename) {
        SimpleGlobUtil::strcpy_s(m_szPathPrefix, MAX_PATH, a_pszFileSpec);
        m_szPathPrefix[pszFilename - a_pszFileSpec + 1] = 0;
    }
#endif

    // search for the first match on the file
    int rc = this->FindFirstFileS(a_pszFileSpec, m_uiFlags);
    if (rc != SG_SUCCESS) {
        if (rc == SG_ERR_NOMATCH && (m_uiFlags & SG_GLOB_NOCHECK)) {
            int ok = AppendName(a_pszFileSpec, false);
            if (ok != SG_SUCCESS) rc = ok;
        }
        return rc;
    }

    // add it and find all subsequent matches
    int nError, nStartLen = m_nArgsLen;
    bool bSuccess;
    do {
        nError = AppendName(this->GetFileNameS((SOCHAR)0), this->IsDirS((SOCHAR)0));
        bSuccess = this->FindNextFileS((SOCHAR)0);
    }
    while (nError == SG_SUCCESS && bSuccess); // NOLINT
    SimpleGlobBase<SOCHAR>::FindDone();

    // sort these files if required
    if (m_nArgsLen > nStartLen && !(m_uiFlags & SG_GLOB_NOSORT)) {
        if (m_uiFlags & SG_GLOB_FULLSORT) {
            nStartLen = m_nReservedSlots;
        }
        SetArgvArrayType(POINTERS);
        qsort(
            m_rgpArgs + nStartLen,
            m_nArgsLen - nStartLen,
            sizeof(m_rgpArgs[0]), fileSortCompare);
    }

    return nError;
}

template <typename SOCHAR>
int
CSimpleGlobTempl<SOCHAR>::Add(
    int a_nCount,
    const SOCHAR* const* a_rgpszFileSpec
    ) {
    int nResult;
    for (int n = 0; n < a_nCount; ++n) {
        nResult = Add(a_rgpszFileSpec[n]);
        if (nResult != SG_SUCCESS) {
            return nResult;
        }
    }
    return SG_SUCCESS;
}

template <typename SOCHAR>
int
CSimpleGlobTempl<SOCHAR>::AppendName(
    const SOCHAR* a_pszFileName,
    bool a_bIsDir
    ) {
    // we need the argv array as offsets in case we resize it
    SetArgvArrayType(OFFSETS);

    // check for special cases which cause us to ignore this entry
    if ((m_uiFlags & SG_GLOB_ONLYDIR) && !a_bIsDir) {
        return SG_SUCCESS;
    }
    if ((m_uiFlags & SG_GLOB_ONLYFILE) && a_bIsDir) {
        return SG_SUCCESS;
    }
    if ((m_uiFlags & SG_GLOB_NODOT) && a_bIsDir) {
        if (a_pszFileName[0] == '.') {
            if (a_pszFileName[1] == '\0') {
                return SG_SUCCESS;
            }
            if (a_pszFileName[1] == '.' && a_pszFileName[2] == '\0') {
                return SG_SUCCESS;
            }
        }
    }

    // ensure that we have enough room in the argv array
    if (!GrowArgvArray(m_nArgsLen + 1)) {
        return SG_ERR_MEMORY;
    }

    // ensure that we have enough room in the string buffer (+1 for null)
    size_t uiPrefixLen = SimpleGlobUtil::strlen(m_szPathPrefix);
    size_t uiLen = uiPrefixLen + SimpleGlobUtil::strlen(a_pszFileName) + 1;
    if (a_bIsDir && (m_uiFlags & SG_GLOB_MARK) == SG_GLOB_MARK) {
        ++uiLen;    // need space for the backslash
    }
    if (!GrowStringBuffer(m_uiBufferLen + uiLen)) {
        return SG_ERR_MEMORY;
    }

    // add this entry. m_uiBufferLen is offset from beginning of buffer.
    m_rgpArgs[m_nArgsLen++] = reinterpret_cast<SOCHAR*>(m_uiBufferLen);
    SimpleGlobUtil::strcpy_s(m_pBuffer + m_uiBufferLen,
                             m_uiBufferSize - m_uiBufferLen, m_szPathPrefix);
    SimpleGlobUtil::strcpy_s(m_pBuffer + m_uiBufferLen + uiPrefixLen,
                             m_uiBufferSize - m_uiBufferLen - uiPrefixLen, a_pszFileName);
    m_uiBufferLen += uiLen;

    // add the directory slash if desired
    if (a_bIsDir && (m_uiFlags & SG_GLOB_MARK) == SG_GLOB_MARK) {
        static const SOCHAR szDirSlash[] = { SG_PATH_CHAR, 0 };
        SimpleGlobUtil::strcpy_s(m_pBuffer + m_uiBufferLen - 2,
                                 m_uiBufferSize - (m_uiBufferLen - 2), szDirSlash);
    }

    return SG_SUCCESS;
}

template <typename SOCHAR>
void
CSimpleGlobTempl<SOCHAR>::SetArgvArrayType(
    ARG_ARRAY_TYPE a_nNewType
    ) {
    if (m_nArgArrayType == a_nNewType) return;
    if (a_nNewType == POINTERS) {
        SG_ASSERT(m_nArgArrayType == OFFSETS);
        for (int n = 0; n < m_nArgsLen; ++n) {
            m_rgpArgs[n] = (m_rgpArgs[n] == reinterpret_cast<SOCHAR*>(-1)) ?
                           nullptr : m_pBuffer + (size_t)m_rgpArgs[n];
        }
    }
    else {
        SG_ASSERT(a_nNewType == OFFSETS);
        SG_ASSERT(m_nArgArrayType == POINTERS);
        for (int n = 0; n < m_nArgsLen; ++n) {
            m_rgpArgs[n] = (m_rgpArgs[n] == nullptr) ?
                           reinterpret_cast<SOCHAR*>(-1) :
                           reinterpret_cast<SOCHAR*>(m_rgpArgs[n] - m_pBuffer);
        }
    }
    m_nArgArrayType = a_nNewType;
}

template <typename SOCHAR>
bool
CSimpleGlobTempl<SOCHAR>::GrowArgvArray(
    int a_nNewLen
    ) {
    if (a_nNewLen >= m_nArgsSize) {
        static const int SG_ARGV_INITIAL_SIZE = 32;
        int nNewSize = (m_nArgsSize > 0) ?
                       m_nArgsSize * 2 : SG_ARGV_INITIAL_SIZE;
        while (a_nNewLen >= nNewSize) {
            nNewSize *= 2;
        }
        void* pNewBuffer = realloc(m_rgpArgs, nNewSize * sizeof(SOCHAR*));
        if (!pNewBuffer) return false;
        m_nArgsSize = nNewSize;
        m_rgpArgs = reinterpret_cast<SOCHAR**>(pNewBuffer);
    }
    return true;
}

template <typename SOCHAR>
bool
CSimpleGlobTempl<SOCHAR>::GrowStringBuffer(
    size_t a_uiMinSize
    ) {
    if (a_uiMinSize >= m_uiBufferSize) {
        static const int SG_BUFFER_INITIAL_SIZE = 1024;
        size_t uiNewSize = (m_uiBufferSize > 0) ?
                           m_uiBufferSize * 2 : SG_BUFFER_INITIAL_SIZE;
        while (a_uiMinSize >= uiNewSize) {
            uiNewSize *= 2;
        }
        void* pNewBuffer = realloc(m_pBuffer, uiNewSize * sizeof(SOCHAR));
        if (!pNewBuffer) return false;
        m_uiBufferSize = uiNewSize;
        m_pBuffer = reinterpret_cast<SOCHAR*>(pNewBuffer);
    }
    return true;
}

template <typename SOCHAR>
int
CSimpleGlobTempl<SOCHAR>::fileSortCompare(
    const void* a1,
    const void* a2
    ) {
    const SOCHAR* s1 = *(const SOCHAR**)a1;
    const SOCHAR* s2 = *(const SOCHAR**)a2;
    if (s1 && s2) {
        return SimpleGlobUtil::strcasecmp(s1, s2);
    }
    // nullptr sorts first
    return s1 == s2 ? 0 : (s1 ? 1 : -1);
}

// ---------------------------------------------------------------------------
//                                  TYPE DEFINITIONS
// ---------------------------------------------------------------------------

/*! \brief ASCII/MBCS version of CSimpleGlob */
using CSimpleGlobA = CSimpleGlobTempl<char>;

/*! \brief wchar_t version of CSimpleGlob */
using CSimpleGlobW = CSimpleGlobTempl<wchar_t>;

#if SG_HAVE_ICU
/*! \brief UChar version of CSimpleGlob */
using CSimpleGlobU = CSimpleGlobTempl<UChar>;
#endif

#ifdef _UNICODE
/*! \brief TCHAR version dependent on if _UNICODE is defined */
# if SG_HAVE_ICU
#  define CSimpleGlob CSimpleGlobU
# else
#  define CSimpleGlob CSimpleGlobW
# endif
#else
/*! \brief TCHAR version dependent on if _UNICODE is defined */
# define CSimpleGlob CSimpleGlobA
#endif

} // namespace glob_local
} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_SIMPLE_GLOB_HEADER

/******************************************************************************/
