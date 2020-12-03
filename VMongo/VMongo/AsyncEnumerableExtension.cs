using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VMongo
{
    public static class AsyncEnumerableExtensions
    {
        public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(this IAsyncCursor<T> asyncCursor)
        {
            while (await asyncCursor.MoveNextAsync())
            {
                foreach (var current in asyncCursor.Current)
                {
                    yield return current;
                }
            }
        }
    }
}
