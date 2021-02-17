//metadoc Common copyright Steve Dekorte 2002
//metadoc Common license BSD revised

#include "Common.h"
#include <stdio.h>

#ifdef IO_CHECK_ALLOC

static long allocs = 0;
static long reallocs = 0;
static long allocatedBytes = 0;
static long maxAllocatedBytes = 0;
static long frees = 0;

/*
typedef struct
{
	long allocs = 0;
	long reallocs = 0;
	long allocatedBytes = 0;
	long maxAllocatedBytes = 0;
	long frees = 0;
} Allocator;

Allocator *Allocator_new(void)
{
	Allocator *self = calloc(1, sizeof(Allocator));
	return self;
}

void Allocator_free(Allocator *self)
{
	free(self);
}

size_t Allocator_allocs(Allocator *self)
{
	return self->allocs;
}

size_t Allocator_frees(Allocator *self)
{
	return self->frees;
}

size_t Allocator_allocatedBytes(Allocator *self)
{
	return self->allocatedBytes;
}

size_t Allocator_maxAllocatedBytes(Allocator *self)
{
	return self->maxAllocatedBytes;
}

void Allocator_resetMaxAllocatedBytes(Allocator *self)
{
	self->maxAllocatedBytes = self->allocatedBytes;
}

void Allocator_show(Allocator *self)
{
	printf("allocs              %i\n", self->allocs);
	printf("reallocs            %i\n", self->reallocs);
	printf("frees               %i\n", self->frees);
	printf("allocsMinusfrees    %i\n", self->allocs - self->frees);
	printf("allocatedBytes      %i\n", self->allocatedBytes);
	printf("maxAllocatedBytes   %i\n", self->maxAllocatedBytes);
	//printf("allocs  %i  bytes   %i\n", self->allocs, self->allocatedBytes);
}

static Allocator *_globalAllocator;

Allocator *globalAllocator(void)
{
	if(!_globalAllocator) Allocator_new();
	return _globalAllocator;
}
*/

// -------------------------------------------------------

typedef struct MemoryBlock MemoryBlock;

struct MemoryBlock
{
	size_t size;
	size_t allocNum;
	char *file;
	int line;
	MemoryBlock *next;
	MemoryBlock *prev;
	char padding[40 - (sizeof(size_t) + sizeof(size_t) + sizeof(char *) + sizeof(int) + sizeof(void *) + sizeof(void *))];
};

MemoryBlock *PtrToMemoryBlock(void *ptr)
{
	return (MemoryBlock *)(((char *)ptr) - sizeof(MemoryBlock));
}

void *MemoryBlockToPtr(MemoryBlock *self)
{
	return (void *)(((char *)self) + sizeof(MemoryBlock));
}

static MemoryBlock *_baseblock = NULL;

//inline 
MemoryBlock *baseblock(void)
{
	if(!_baseblock) _baseblock = calloc(1, sizeof(MemoryBlock));
	return _baseblock;
}

void MemoryBlock_remove(MemoryBlock *self)
{
	if (self->next) self->next->prev = self->prev;
	if (self->prev) self->prev->next = self->next;
}

void MemoryBlock_insertAfter_(MemoryBlock *self, MemoryBlock *other)
{
	self->next = other->next;
	self->prev = other;
	other->next = self;
	if (self->next) self->next->prev = self;
}

MemoryBlock *MemoryBlock_newWithSize_file_line_(size_t size, char *file, int line)
{
	MemoryBlock *self = calloc(1, sizeof(MemoryBlock) + size);
	self->size = size;
	self->allocNum = allocs;
	self->file = file;
	self->line = line;
	MemoryBlock_insertAfter_(self, baseblock());

	allocs ++;
	allocatedBytes += size;
	if (allocatedBytes > maxAllocatedBytes) maxAllocatedBytes = allocatedBytes;
	return self;
}

MemoryBlock *MemoryBlock_reallocToSize_(MemoryBlock *self, size_t size)
{
	MemoryBlock *prev = self->prev;
	MemoryBlock_remove(self);
	allocatedBytes -= self->size;
	allocatedBytes += size;
	reallocs ++;
	self = realloc(self, sizeof(MemoryBlock) + size);
	self->size = size;
	MemoryBlock_insertAfter_(self, prev);
	return self;
}

void MemoryBlock_free(MemoryBlock *self)
{
	MemoryBlock_remove(self);
	allocatedBytes -= self->size;
	frees ++;
	free(self);
}

size_t MemoryBlock_size(MemoryBlock *self)
{
	return self->size;
}

void MemoryBlock_show(MemoryBlock *self)
{
	char *file = strrchr(self->file, '/');
	file = file ? file + 1 : self->file;
	//printf("  MemoryBlock %p:\n", (void *)self);
	//printf("\tsize %i\n", self->size);
	//printf("\tfile %s\n", file);
	//printf("\tline %i\n", self->line);
	printf("\t%i %p %s:%i\t\t%i bytes\n", self->allocNum, MemoryBlockToPtr(self), file, self->line, self->size);
}

// ----------------------------------------------------------------------------

void *io_real_malloc(size_t size, char *file, int line)
{
	MemoryBlock *m = MemoryBlock_newWithSize_file_line_(size, file, line);
	return MemoryBlockToPtr(m);
}

void *io_real_calloc(size_t count, size_t size, char *file, int line)
{
	return io_real_malloc(count * size, file, line);
}

void *io_real_realloc(void *ptr, size_t size, char *file, int line)
{
	if (ptr)
	{
		MemoryBlock *m = MemoryBlock_reallocToSize_(PtrToMemoryBlock(ptr), size);
		return MemoryBlockToPtr(m);
	}

	return io_real_malloc(size, file, line);
}

void io_free(void *ptr)
{
	MemoryBlock_free(PtrToMemoryBlock(ptr));
}

// --------------------------------------------------------------------------

void io_show_mem(char *s)
{
	printf("\n--- %s ---\n", s ? s : "");
	printf("allocs              %i\n", allocs);
	printf("reallocs            %i\n", reallocs);
	printf("frees               %i\n", frees);
	printf("allocsMinusfrees    %i\n", allocs - frees);
	printf("allocatedBytes      %i\n", allocatedBytes);
	printf("maxAllocatedBytes   %i\n", maxAllocatedBytes);
	//printf("allocs  %i  bytes   %i\n", allocs, allocatedBytes);
	//printf("\n");
}

size_t io_maxAllocatedBytes(void)
{
	return maxAllocatedBytes;
}

void io_resetMaxAllocatedBytes(void)
{
	maxAllocatedBytes = allocatedBytes;
}

size_t io_frees(void)
{
	return frees;
}

size_t io_allocatedBytes(void)
{
	return allocatedBytes;
}

size_t io_allocs(void)
{
	return allocs;
}

void io_showUnfreed(void)
{
	MemoryBlock *m = baseblock()->next;
	size_t sum = 0;
	int n = 0;

	while (m)
	{
		MemoryBlock_show(m);
		sum += m->size;
		n ++;
		m = m->next;
	}

	printf("\n  %i bytes in %i blocks\n", (int)sum, n);
}

#endif

void *cpalloc(const void *p, size_t size)
{
	void *n = io_malloc(size);
	if(p) memcpy(n, p, size);
	return n;
}

void *io_freerealloc(void *p, size_t size)
{
	return realloc(p, size);
	/*
	void *n = io_malloc(size);

	if (p != NULL)
	{
		memcpy(n, p, size);
		free(p);
	}

	return n;
	*/
}

int io_isBigEndian(void)
{
	int i = 0x1;
	uint8_t *s = (uint8_t *)(&i);
	return s[0];
}

uint32_t io_uint32InBigEndian(uint32_t i)
{
	uint32_t o;
	uint8_t *os = (uint8_t *)&o;
	uint8_t *is = (uint8_t *)&i;
	if (io_isBigEndian()) return i;
	os[0] = is[3];
	os[1] = is[2];
	os[2] = is[1];
	os[3] = is[0];
	return o;
}

