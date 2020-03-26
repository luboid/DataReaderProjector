using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataReaderProjector.Dynamic
{
    class DynDeserializerItem
    {
        public DynDeserializerItem()
        {
            Index = -1;
            Fields = new Dictionary<string, int>();
            Collections = new Dictionary<string, DynDeserializerItem>();
        }

        public int Index = -1;
        public bool Single = false;

        public Dictionary<string, int> Fields { get; set; }

        public Dictionary<string, DynDeserializerItem> Collections { get; set; }
    }
}
