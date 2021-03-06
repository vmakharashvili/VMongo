﻿using MongoDB.Driver;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace VMongo
{
    public abstract class AggregateRepositoryBase<Aggregate, Filter, IdType> : CommonVMongoBase<Aggregate, Filter, IdType>
        where Aggregate : IVMongoEntity<IdType>
    {
        protected AggregateRepositoryBase(VMongoContext context) : base(context)
        {

        }

        public async Task<IdType> CreateAsync(Aggregate entity)
        {
            entity.Id = await GetUintSequenceValue();
            await Collection.InsertOneAsync(entity);
            return entity.Id;
        }

        public async Task DeleteAsync(IdType id)
        {
            var filter = F.Eq(x => x.Id, id);
            await Collection.DeleteOneAsync(filter);
        }

        public Task UpdateAsync(Aggregate aggregate)
        {
            var filter = F.Eq(x => x.Id, aggregate.Id);
            return Collection.ReplaceOneAsync(filter, aggregate);
        }

        public Task<Aggregate> GetByIdAsync(IdType id)
        {
            var filter = F.Eq(x => x.Id, id);
            return Collection.Find(filter).FirstOrDefaultAsync();
        }

        public Task<List<Aggregate>> GetListAsync(Filter filter)
        {
            var filterDef = ListFilter(filter, F.Empty);
            return Collection.Find(filterDef).ToListAsync();
        }

        public IAsyncEnumerable<Aggregate> GetStreamAsync(Filter filter)
        {
            var filterDef = ListFilter(filter, F.Empty);
            return Collection.Find(filterDef).ToCursor().ToAsyncEnumerable();
        }

        public async Task<(List<Aggregate> list, long totalRecords)> GetPagedListAsync(int skip, int take, Filter filter)
        {
            var filterDef = ListFilter(filter, F.Empty);
            var list = await Collection.Find(filterDef).Skip(skip * take).Limit(take).ToListAsync();
            var total = await Collection.Find(filterDef).CountDocumentsAsync();
            return (list, total);
        }
    }
}
