#pragma once

#include <stddef.h>


/** Позволяет считать количество различных событий, произошедших в программе
  *  - для высокоуровневого профайлинга.
  */

#define APPLY_FOR_EVENTS(M) \
	M(Query) \
	M(SelectQuery) \
	M(InsertQuery) \
	M(FileOpen) \
	M(Seek) \
	M(ReadBufferFromFileDescriptorRead) \
	M(ReadCompressedBytes) \
	M(CompressedReadBufferBlocks) \
	M(CompressedReadBufferBytes) \
	M(UncompressedCacheHits) \
	M(UncompressedCacheMisses) \
	M(UncompressedCacheWeightLost) \
	M(IOBufferAllocs) \
	M(IOBufferAllocBytes) \
	M(ArenaAllocChunks) \
	M(ArenaAllocBytes) \
	M(FunctionExecute) \
	M(MarkCacheHits) \
	M(MarkCacheMisses) \
	\
	M(ReplicatedPartFetches) \
	M(ReplicatedPartFailedFetches) \
	M(ObsoleteReplicatedParts) \
	M(ReplicatedPartMerges) \
	M(ReplicatedPartFetchesOfMerged) \
	M(ReplicatedPartChecks) \
	M(ReplicatedPartChecksFailed) \
	M(ReplicatedDataLoss) \
	\
	M(DelayedInserts) \
	M(RejectedInserts) \
	M(DelayedInsertsMilliseconds) \
	\
	M(ZooKeeperInit) \
	M(ZooKeeperTransactions) \
	M(ZooKeeperGetChildren) \
	M(ZooKeeperCreate) \
	M(ZooKeeperRemove) \
	M(ZooKeeperExists) \
	M(ZooKeeperGet) \
	M(ZooKeeperSet) \
	M(ZooKeeperMulti) \
	M(ZooKeeperExceptions) \
	\
	M(END)

namespace ProfileEvents
{
	/// Виды событий.
	enum Event
	{
	#define M(NAME) NAME,
		APPLY_FOR_EVENTS(M)
	#undef M
	};


	/// Получить текстовое описание события по его enum-у.
	inline const char * getDescription(Event event)
	{
		static const char * descriptions[] =
		{
		#define M(NAME) #NAME,
			APPLY_FOR_EVENTS(M)
		#undef M
		};

		return descriptions[event];
	}


	/// Счётчики - сколько раз каждое из событий произошло.
	extern size_t counters[END];


	/// Увеличить счётчик события. Потокобезопасно.
	inline void increment(Event event, size_t amount = 1)
	{
		__sync_fetch_and_add(&counters[event], amount);
	}
}


#undef APPLY_FOR_EVENTS
