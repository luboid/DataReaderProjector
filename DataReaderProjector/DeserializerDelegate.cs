using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace DataReaderProjector
{
    public delegate T DeserializerDelegate<T>(DbDataReader reader, out bool more);
}
