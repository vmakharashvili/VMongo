using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VMongo
{
    public abstract class AggregateRepositoryBase<Aggregate, Filter, IdType>
        where Aggregate: ISoftDeletableVMongoEntity<IdType>
    {
        protected readonly RepositoryContext Context;

        protected AggregateRepositoryBase(RepositoryContext context)
        {
            Context = context;
        }

        protected FilterDefinitionBuilder<Aggregate> F => Builders<Aggregate>.Filter;
        protected UpdateDefinitionBuilder<Aggregate> U => Builders<Aggregate>.Update;

        protected abstract string CollectionName { get; }
        protected abstract FilterDefinition<Aggregate> ListFilter(Filter filter, FilterDefinition<Aggregate> filterDef);

        protected IMongoCollection<Aggregate> Collection => Context.MongoDatabase.GetCollection<Aggregate>(CollectionName);

        protected async Task<IdType> GetUintSequenceValue()
        {
            var existed = await Context.MongoDatabase.GetCollection<Sequence>("Sequence").Find(x => x.Name == CollectionName).FirstOrDefaultAsync();
            if(existed == null)
            {
                var newId = GenerateNew();
                await Context.MongoDatabase.GetCollection<Sequence>("Sequence").InsertOneAsync(new Sequence { Name = CollectionName, Value = newId.Item1 });
                return newId.Item2;
            }
            else
            {
                var id = existed.Value;
                var generated = GenerateNext(id!);
                var filter = Builders<Sequence>.Filter.Eq(x => x.Name, CollectionName);
                var update = Builders<Sequence>.Update.Set(x => x.Value, generated.Item1);
                var result = Context.MongoDatabase.GetCollection<Sequence>("Sequence").FindOneAndUpdate(filter, update,
                    new FindOneAndUpdateOptions<Sequence, Sequence>
                    {
                        IsUpsert = true,
                        ReturnDocument = ReturnDocument.After
                    });
                return generated.Item2;

            }
        }

        private static (string, IdType) GenerateNext(string existedValue)
        {
            if (typeof(IdType) == typeof(int))
            {
                var existed = Convert.ToInt32(existedValue);
                var nv = existed++;
                return (nv.ToString(), (IdType)Convert.ChangeType(nv, typeof(IdType)));
            }

            if (typeof(IdType) == typeof(uint))
            {
                var existed = Convert.ToUInt32(existedValue);
                var nv = existed++;
                return (nv.ToString(), (IdType)Convert.ChangeType(nv, typeof(IdType)));
            }

            if (typeof(IdType) == typeof(long))
            {
                var existed = Convert.ToInt64(existedValue);
                var nv = existed++;
                return (nv.ToString(), (IdType)Convert.ChangeType(nv, typeof(IdType)));
            }

            if (typeof(IdType) == typeof(ulong))
            {
                var existed = Convert.ToUInt64(existedValue);
                var nv = existed++;
                return (nv.ToString(), (IdType)Convert.ChangeType(nv, typeof(IdType)));
            }

            if (typeof(IdType) == typeof(Guid))
            {
                var newGuid = Guid.NewGuid();
                return (newGuid.ToString(), (IdType)Convert.ChangeType(newGuid, typeof(IdType)));
            }
            else
            {
                throw new NotImplementedException($"{typeof(IdType).Name} not supported");
            }
        }

        private static (string, IdType) GenerateNew()
        {
            if(typeof(IdType) == typeof(int) 
                || typeof(IdType) == typeof(uint) 
                || typeof(IdType) == typeof(long)
                || typeof(IdType) == typeof(ulong))
            {
                return ("1", (IdType)Convert.ChangeType(1, typeof(IdType)));
            }

            if(typeof(IdType) == typeof(Guid))
            {
                var newGuid = Guid.NewGuid();
                return (newGuid.ToString(), (IdType)Convert.ChangeType(newGuid, typeof(IdType)));
            }
            else
            {
                throw new NotImplementedException($"{typeof(IdType).Name} not supported");
            }
        }

        public async Task<IdType> Create(Aggregate entity)
        {
            entity.Id = await GetUintSequenceValue();
            await Collection.InsertOneAsync(entity);
            return entity.Id;
        }

        public async Task HardDelete(IdType id)
        {
            var filter = F.Eq(x => x.Id, id);
            await Collection.DeleteOneAsync(filter);
        }

        public async Task SoftDelete(IdType id)
        {
            var filter = F.Eq(x => x.Id, id);
            var update = U.Set(x => x.IsDeleted, true);
            await Collection.UpdateOneAsync(filter, update);
        }

        public Task Update(Aggregate aggregate)
        {
            var filter = F.Eq(x => x.Id, aggregate.Id);
            return Collection.ReplaceOneAsync(filter, aggregate);
        }

        public Task<Aggregate> GetById(IdType id)
        {
            var filter = F.Eq(x => x.Id, id);
            return Collection.Find(filter).FirstOrDefaultAsync();
        }

        public Task<List<Aggregate>> GetList(Filter filter)
        {
            var filterDef = F.Eq(x => x.IsDeleted, false);
            filterDef &= ListFilter(filter, filterDef);
            return Collection.Find(filterDef).ToListAsync();
        }

        public IAsyncEnumerable<Aggregate> GetStream(Filter filter)
        {
            var filterDef = F.Eq(x => x.IsDeleted, false);
            filterDef &= ListFilter(filter, filterDef);
            return Collection.Find(filterDef).ToCursor().ToAsyncEnumerable();
        }

        public async Task<(List<Aggregate> list, long totalRecords)> GetPagedList(int skip, int take, Filter filter)
        {
            var filterDef = F.Eq(x => x.IsDeleted, false);
            filterDef &= ListFilter(filter, filterDef);
            var list = await Collection.Find(filterDef).Skip(skip * take).Limit(take).ToListAsync();
            var total = await Collection.Find(filterDef).CountDocumentsAsync();
            return (list, total);
        }
    }
}
