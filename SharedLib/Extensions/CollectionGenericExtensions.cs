using System.Collections.Generic;
using System.Linq;

namespace SharedLib.Extensions
{
    public static class CollectionGenericExtensions
    {
        public static IEnumerable<IEnumerable<T>> Paged<T>(this IEnumerable<T> source, int size)
        {
            return source.Select((item, index) => (item, index / size))
                .GroupBy(x => x.Item2)
                .Select(gr => gr.AsEnumerable()
                    .Select(x => x.item)
                );
        }
    }
}