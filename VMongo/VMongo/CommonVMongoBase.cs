using MongoDB.Driver;
using System;
using System.Threading.Tasks;

namespace VMongo
{
    public abstract class CommonVMongoBase<Aggregate, Filter, IdType>
    {
        protected readonly VMongoContext Context;

        protected CommonVMongoBase(VMongoContext context)
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
            if (existed == null)
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
            if (typeof(IdType) == typeof(int)
                || typeof(IdType) == typeof(uint)
                || typeof(IdType) == typeof(long)
                || typeof(IdType) == typeof(ulong))
            {
                return ("1", (IdType)Convert.ChangeType(1, typeof(IdType)));
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

    }
}
