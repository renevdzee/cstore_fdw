/*-------------------------------------------------------------------------
 *
 * cstore_compression.c
 *
 * This file contains compression/decompression functions definitions
 * used in cstore_fdw.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cstore_fdw.h"

#if PG_VERSION_NUM >= 90500
#include "common/pg_lzcompress.h"
#else
#include "utils/pg_lzcompress.h"
#endif

#ifdef ENABLE_LZ4
#include <lz4.h>
#include <lz4hc.h>
#endif

#ifdef ENABLE_ZSTD
#include <zstd.h>
#endif



#if PG_VERSION_NUM >= 90500
/*
 *	The information at the start of the compressed data. This decription is taken
 *	from pg_lzcompress in pre-9.5 version of PostgreSQL.
 */
typedef struct CStoreCompressHeader
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int32		rawsize;
} CStoreCompressHeader;

/*
 * Utilities for manipulation of header information for compressed data
 */

#define CSTORE_COMPRESS_HDRSZ		((int32) sizeof(CStoreCompressHeader))
#define CSTORE_COMPRESS_RAWSIZE(ptr) (((CStoreCompressHeader *) (ptr))->rawsize)
#define CSTORE_COMPRESS_RAWDATA(ptr) (((char *) (ptr)) + CSTORE_COMPRESS_HDRSZ)
#define CSTORE_COMPRESS_SET_RAWSIZE(ptr, len) (((CStoreCompressHeader *) (ptr))->rawsize = (len))

#else

#define CSTORE_COMPRESS_HDRSZ		(0)
#define CSTORE_COMPRESS_RAWSIZE(ptr) (PGLZ_RAW_SIZE((PGLZ_Header *) buffer->data))
#define CSTORE_COMPRESS_RAWDATA(ptr) (((PGLZ_Header *) (ptr)))
#define CSTORE_COMPRESS_SET_RAWSIZE(ptr, len) (((CStoreCompressHeader *) (ptr))->rawsize = (len))

#endif



/*
 * CompressBuffer compresses the given buffer with the given compression type
 * outputBuffer enlarged to contain compressed data. The function returns the actual
 * compressionType if compression is done, returns COMPRESSION_NONE if compression is not done.
 * outputBuffer is valid only if the function returns true.
 */
CompressionType
CompressBuffer(StringInfo inputBuffer, StringInfo outputBuffer,
			   CompressionType compressionType, int compressionLevel)
{
	uint64 maximumLength;
	CompressionType compressionResult = COMPRESSION_NONE;

#if PG_VERSION_NUM >= 90500
	int32 compressedByteCount = 0;
#endif

	if (compressionType == COMPRESSION_NONE)
	{
		return COMPRESSION_NONE;
	}

	resetStringInfo(outputBuffer);

	switch(compressionType) {

	case COMPRESSION_PG_LZ:
		maximumLength = PGLZ_MAX_OUTPUT(inputBuffer->len) + CSTORE_COMPRESS_HDRSZ;
		enlargeStringInfo(outputBuffer, maximumLength);

#if PG_VERSION_NUM >= 90500
		compressedByteCount = pglz_compress((const char *) inputBuffer->data,
											inputBuffer->len,
											CSTORE_COMPRESS_RAWDATA(outputBuffer->data),
											PGLZ_strategy_always);
		if (compressedByteCount >= 0)
		{
			CSTORE_COMPRESS_SET_RAWSIZE(outputBuffer->data, inputBuffer->len);
			SET_VARSIZE_COMPRESSED(outputBuffer->data,
								   compressedByteCount + CSTORE_COMPRESS_HDRSZ);
			compressionResult = COMPRESSION_PG_LZ;
		}
#else

		compressionResult = pglz_compress(inputBuffer->data, inputBuffer->len,
									  CSTORE_COMPRESS_RAWDATA(outputBuffer->data),
									  PGLZ_strategy_always) ? COMPRESSION_PG_LZ : COMPRESSION_NONE;
#endif
		break;

#ifdef ENABLE_LZ4
	case COMPRESSION_LZ4:
		maximumLength = inputBuffer->len;
		enlargeStringInfo(outputBuffer, maximumLength);

		if (compressionLevel == 0)
		{
			compressedByteCount = LZ4_compress_default((const char *) inputBuffer->data,
													   CSTORE_COMPRESS_RAWDATA(outputBuffer->data),
													   inputBuffer->len,
													   maximumLength - CSTORE_COMPRESS_HDRSZ);
		}
		else
		{
			compressedByteCount = LZ4_compress_HC((const char *) inputBuffer->data,
												CSTORE_COMPRESS_RAWDATA(outputBuffer->data),
												inputBuffer->len,
												maximumLength - CSTORE_COMPRESS_HDRSZ,
												compressionLevel);
		}

		if (compressedByteCount > 0)
		{
			CSTORE_COMPRESS_SET_RAWSIZE(outputBuffer->data, inputBuffer->len);
			SET_VARSIZE_COMPRESSED(outputBuffer->data,
								   compressedByteCount + CSTORE_COMPRESS_HDRSZ);
			compressionResult = COMPRESSION_LZ4;
		}

		break;
#endif

#ifdef ENABLE_ZSTD
	case COMPRESSION_ZSTD:
		maximumLength = inputBuffer->len;
		enlargeStringInfo(outputBuffer, maximumLength);
		if (compressionLevel==0) compressionLevel = 1;

		compressedByteCount = ZSTD_compress(CSTORE_COMPRESS_RAWDATA(outputBuffer->data),
											maximumLength - CSTORE_COMPRESS_HDRSZ,
											(const char *) inputBuffer->data,
											inputBuffer->len,
											compressionLevel);

		if (!ZSTD_isError(compressedByteCount))
		{
			CSTORE_COMPRESS_SET_RAWSIZE(outputBuffer->data, inputBuffer->len);
			SET_VARSIZE_COMPRESSED(outputBuffer->data,
								   compressedByteCount + CSTORE_COMPRESS_HDRSZ);
			compressionResult = COMPRESSION_ZSTD;
		}

		break;
#endif

	default:
		break;

	}


	if (compressionResult != COMPRESSION_NONE)
	{
		outputBuffer->len = VARSIZE(outputBuffer->data);
	}

	return compressionResult;
}


/*
 * DecompressBuffer decompresses the given buffer with the given compression
 * type. This function returns the buffer as-is when no compression is applied.
 */
StringInfo
DecompressBuffer(StringInfo buffer, CompressionType compressionType)
{
	StringInfo decompressedBuffer = NULL;

	Assert(compressionType == COMPRESSION_NONE
		|| compressionType == COMPRESSION_PG_LZ
#ifdef ENABLE_LZ4
		|| compressionType == COMPRESSION_LZ4
#endif
#ifdef ENABLE_ZSTD
		|| compressionType == COMPRESSION_ZSTD
#endif
		);

	if (compressionType == COMPRESSION_NONE)
	{
		/* in case of no compression, return buffer */
		decompressedBuffer = buffer;
	}
	else if (compressionType == COMPRESSION_PG_LZ)
	{
		uint32 compressedDataSize = VARSIZE(buffer->data) - CSTORE_COMPRESS_HDRSZ;
		uint32 decompressedDataSize = CSTORE_COMPRESS_RAWSIZE(buffer->data);
		char *decompressedData = NULL;
#if PG_VERSION_NUM >= 90500
		int32 decompressedByteCount = 0;
#endif

		if (compressedDataSize + CSTORE_COMPRESS_HDRSZ != buffer->len)
		{
			ereport(ERROR, (errmsg("cannot decompress the buffer"),
							errdetail("Expected %u bytes, but received %u bytes",
									  compressedDataSize, buffer->len)));
		}

		decompressedData = palloc0(decompressedDataSize);

#if PG_VERSION_NUM >= 90500

#if PG_VERSION_NUM >= 120000
		decompressedByteCount = pglz_decompress(CSTORE_COMPRESS_RAWDATA(buffer->data),
												compressedDataSize, decompressedData,
												decompressedDataSize, true);
#else
		decompressedByteCount = pglz_decompress(CSTORE_COMPRESS_RAWDATA(buffer->data),
												compressedDataSize, decompressedData,
												decompressedDataSize);
#endif

		if (decompressedByteCount < 0)
		{
			ereport(ERROR, (errmsg("cannot decompress the buffer"),
							errdetail("compressed data is corrupted")));
		}
#else
		pglz_decompress((PGLZ_Header *) buffer->data, decompressedData);
#endif

		decompressedBuffer = palloc0(sizeof(StringInfoData));
		decompressedBuffer->data = decompressedData;
		decompressedBuffer->len = decompressedDataSize;
		decompressedBuffer->maxlen = decompressedDataSize;
	}
	else if (compressionType == COMPRESSION_LZ4)
	{
#ifdef ENABLE_LZ4
		uint32 compressedDataSize = VARSIZE(buffer->data) - CSTORE_COMPRESS_HDRSZ;
		uint32 decompressedDataSize = CSTORE_COMPRESS_RAWSIZE(buffer->data);
		char* decompressedData = palloc0(decompressedDataSize);

		int32 decompressedByteCount = LZ4_decompress_safe(CSTORE_COMPRESS_RAWDATA(buffer->data),
												decompressedData,
												compressedDataSize,
												decompressedDataSize);

		if (decompressedByteCount < 0)
		{
			ereport(ERROR, (errmsg("cannot decompress the buffer"),
							errdetail("lz4 compressed data is corrupted")));
		}

		decompressedBuffer = palloc0(sizeof(StringInfoData));
		decompressedBuffer->data = decompressedData;
		decompressedBuffer->len = decompressedDataSize;
		decompressedBuffer->maxlen = decompressedDataSize;
#else
		ereport(ERROR, (errmsg("cannot decompress block, lz4 compression is disabled")));
#endif
	}
	else if (compressionType == COMPRESSION_ZSTD)
	{
#ifdef ENABLE_ZSTD
		uint32 compressedDataSize = VARSIZE(buffer->data) - CSTORE_COMPRESS_HDRSZ;
		uint32 decompressedDataSize = CSTORE_COMPRESS_RAWSIZE(buffer->data);
		char* decompressedData = palloc0(decompressedDataSize);

		int32 decompressedByteCount = ZSTD_decompress(decompressedData,
												decompressedDataSize,
												CSTORE_COMPRESS_RAWDATA(buffer->data),
												compressedDataSize);
		if (ZSTD_isError(decompressedByteCount))
		{
			ereport(ERROR, (errmsg("cannot decompress the buffer"),
							errdetail("zstd compressed data is corrupted, %s", ZSTD_getErrorName(decompressedByteCount)
			)));
		}

		decompressedBuffer = palloc0(sizeof(StringInfoData));
		decompressedBuffer->data = decompressedData;
		decompressedBuffer->len = decompressedDataSize;
		decompressedBuffer->maxlen = decompressedDataSize;
#else
		ereport(ERROR, (errmsg("cannot decompress block, zstd compression is disabled")));
#endif
	}
	else
	{
		ereport(ERROR, (errmsg("cannot decompress block"),
						errdetail("compression algorithm %d not supported",
							  compressionType)));
	}

	return decompressedBuffer;
}
