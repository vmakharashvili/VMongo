using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace VMongo
{
    public abstract class SoftDeletableAggregateRepositoryBase<Aggregate, Filter, IdType> : CommonVMongoBase<Aggregate, Filter, IdType>
        where Aggregate : ISoftDeletableVMongoEntity<IdType>
    {
        protected SoftDeletableAggregateRepositoryBase(VMongoContext context): base(context)
        {

        }
       
        public async Task<IdType> CreateAsync(Aggregate entity)
        {
            entity.Id = await GetUintSequenceValue();
            await Collection.InsertOneAsync(entity);
            return entity.Id;
        }

        public async Task HardDeleteAsync(IdType id)
        {
            var filter = F.Eq(x => x.Id, id);
            await Collection.DeleteOneAsync(filter);
        }

        public async Task SoftDeleteAsync(IdType id)
        {
            var filter = F.Eq(x => x.Id, id);
            var update = U.Set(x => x.IsDeleted, true);
            await Collection.UpdateOneAsync(filter, update);
        }

        public Task UpdateAsync(Aggregate aggregate)
        {
            var filter = F.Eq(x => x.Id, aggregate.Id);
            filter &= F.Eq(x => x.IsDeleted, false);
            return Collection.ReplaceOneAsync(filter, aggregate);
        }

        public Task<Aggregate> GetByIdAsync(IdType id)
        {
            var filter = F.Eq(x => x.Id, id);
            filter &= F.Eq(x => x.IsDeleted, false);
            return Collection.Find(filter).FirstOrDefaultAsync();
        }

        public Task<List<Aggregate>> GetListAsync(Filter filter)
        {
            var filterDef = F.Eq(x => x.IsDeleted, false);
            filterDef &= ListFilter(filter, filterDef);
            return Collection.Find(filterDef).ToListAsync();
        }

        public IAsyncEnumerable<Aggregate> GetStreamAsync(Filter filter)
        {
            var filterDef = F.Eq(x => x.IsDeleted, false);
            filterDef &= ListFilter(filter, filterDef);
            return Collection.Find(filterDef).ToCursor().ToAsyncEnumerable();
        }

        public async Task<(List<Aggregate> list, long totalRecords)> GetPagedListAsync(int skip, int take, Filter filter)
        {
            var filterDef = F.Eq(x => x.IsDeleted, false);
            filterDef &= ListFilter(filter, filterDef);
            var list = await Collection.Find(filterDef).Skip(skip * take).Limit(take).ToListAsync();
            var total = await Collection.Find(filterDef).CountDocumentsAsync();
            return (list, total);
        }
    }
}
