using MongoDB.Driver;
using System;
using System.Threading.Tasks;

namespace VMongo
{
    public class VMongoContext
    {
        private readonly MongoClient _mongoClient;
        public readonly IMongoDatabase MongoDatabase;

        public VMongoContext(string connectionString, string dataBase)
        {
            _mongoClient = new MongoClient(connectionString);
            MongoDatabase = _mongoClient.GetDatabase(dataBase);
        }

        public async Task ExecuteWithTransaction(Func<Task> action, Action<Exception> handleException)
        {
            using var session = await _mongoClient.StartSessionAsync();
            try
            {
                session.StartTransaction();
                await action();
                await session.CommitTransactionAsync();
            }
            catch (Exception ex)
            {
                await session.AbortTransactionAsync();
                handleException(ex);
                throw;
            }
        }

        public async Task<R> ExecuteWithTransaction<R>(Func<Task<R>> action, Action<Exception> handleException)
        {
            using var session = await _mongoClient.StartSessionAsync();
            try
            {
                session.StartTransaction();
                var r = await action();
                await session.CommitTransactionAsync();
                return r;
            }
            catch (Exception ex)
            {
                await session.AbortTransactionAsync();
                handleException(ex);
                throw;
            }
        }
    }
}
