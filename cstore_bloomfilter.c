#include <math.h>
#include <string.h>
#include "postgres.h"
#include "access/nbtree.h"
#include "cstore_fdw.h"
#include "murmur3.h"

/* bitset functions */

static void
BitSet_set(uint64_t *bitSet, uint64_t pos)
{
	int bit = pos & 63;
	int idx = pos / 64;
	bitSet[idx] |= (1ull << bit);
}

static bool
BitSet_ismember(uint64_t *bitSet, uint64_t pos)
{
	int bit = pos & 63;
	int idx = pos / 64;
	return bitSet[idx] & (1ull << bit);
}

#if 0
static int BitSet_half(uint64_t* bitSet, int len)
{
    int half = len/128;
    for(int idx=0;idx<half;idx++)
      bitSet[idx]|=bitSet[half+idx];
    return half*64;
}
#endif

/* Hash functions */

// Thomas Wang's long hash function:
// see http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm
static uint64_t
hash64shift(uint64_t key)
{
	key = (~key) + (key << 21); // key = (key << 21) - key - 1;
	key = key ^ (key >> 24);
	key = key * 265; // (key + (key << 3)) + (key << 8)
	key = key ^ (key >> 14);
	key = key * 21; // (key + (key << 2)) + (key << 4)
	key = key ^ (key >> 28);
	key = key + (key << 31);
	return key;
}

static uint64_t
MurmurHash3_x64_64(const void *data, int len, uint32_t seed)
{
	uint64_t out[2];
	MurmurHash3_x64_128(data, len, seed, out);
	return out[0];
}

/* Hash PostgreSQL Datum  */

uint64
DatumHash64(Datum columnValue, bool columnTypeByValue, int columnTypeLength)
{
	if (columnTypeLength > 0)
	{
		if (columnTypeByValue)
			return hash64shift(columnValue);
		else
			return MurmurHash3_x64_64(DatumGetPointer(columnValue), columnTypeLength, 0);
	}
	else
	{
		Assert(!datumTypeByValue);
		return MurmurHash3_x64_64(VARDATA_ANY(columnValue), VARSIZE_ANY_EXHDR(columnValue), 0);
	}
}

/* Bloomfilter */

/* Allocates and initializes an empty bloom filter */
void
BloomFilter_init(BloomFilter *bloomFilter, int numBits, int numHashFunctions)
{
	/* round the number of bits up to a multiple of 64 */
	int m = numBits - (numBits + 63) % 64 + 63;

	bloomFilter->numBits = m;
	bloomFilter->bitSet = palloc0(m / 8);
	bloomFilter->numHashFunctions = numHashFunctions;
}

/* Sizes and constructs a bloomfilter based on the expected number of unique
   values (in a chunk) and the requested false-positive rate.
   (can be optimized, see http://peterd.org/pcd-diss.pdf)
*/
void
BloomFilter_autoSize(BloomFilter *bloomFilter, int uniquevalues, float falsepositiverate)
{
	int m, k;

	/* calculate number of bits */
	m = uniquevalues * (-log(falsepositiverate) / (log(2) * log(2)));

	/* round up to a multiple of 64 bits */
	m = m - (m + 63) % 64 + 63;

	/* calculate number of hash functions */
	k = (m / uniquevalues) * log(2);

	BloomFilter_init(bloomFilter, m, k);
}

/* Adds an element to the bloom filter.
   uses enhanced double-hashing, https://en.wikipedia.org/wiki/Double_hashing
*/
void
BloomFilter_addHash(BloomFilter *bloomFilter, uint64 hash)
{
	uint32 a = (uint32)(hash & 0xffffffff);
	uint32 b = (uint32)(hash >> 32);

	for (int i = 0; i < bloomFilter->numHashFunctions; i++)
	{
		BitSet_set(bloomFilter->bitSet, a % bloomFilter->numBits);
		a += b;
		b += i;
	}
}

/* Tests if an hash value is maybe in the bloom filter */
bool
BloomFilter_testHash(BloomFilter *bloomFilter, uint64 hash)
{
	uint32 a = (uint32)(hash & 0xffffffff);
	uint32 b = (uint32)(hash >> 32);

	for (int i = 0; i < bloomFilter->numHashFunctions; i++)
	{
		if (!BitSet_ismember(bloomFilter->bitSet, a % bloomFilter->numBits))
		{
			return false;
		}
		a += b;
		b += i;
	}
	return true;
}

bool
CStoreParseBloomfilterOption(const char *optionValue, int *uniquevalues, float *falsepositiverate)
{
	*uniquevalues = 0;
	*falsepositiverate = BLOOMFILTER_DEFAULT_FALSEPOSITIVE;

	if ( sscanf(optionValue, "%d,%f", uniquevalues, falsepositiverate) == 0)
		return false;
	else
		return true;
}
