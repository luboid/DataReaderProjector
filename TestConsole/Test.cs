using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Data.Common;
using System.Data;
using DataReaderProjector;
using TestConsole.Model;
using Oracle.ManagedDataAccess.Client;

namespace TestConsole
{
    static class Test
    {
        static string commandText = @"select * from (
    select t.csuniq, t.csname, t.egn_ekpou,
           a.uniqid cauniq, a.cakind, a.cacity, a.capk,
           z.hivalue, z.hivdescr,
           pt.hivalue ptid, pt.hivdescr ptdescr,
           p.uniqid cpuniq, p.cpkind, p.cpareacode, p.cpphone, p.cpcontact
      from (select hivalue, hivdescr
              from bshintvalues
             where hiuniq = clients.typeaddr) z,
           (select hivalue, hivdescr
              from bshintvalues
             where hiuniq = clients.phonetype) pt,
           csphones p,
           csaddresses a, cscommon t
     where z.hivalue(+) = a.cakind
       and pt.hivalue(+) = p.cpkind
       and a.csuniq(+) = t.csuniq
       and p.csuniq(+) = t.csuniq
     order by t.csuniq, cauniq, cpuniq
) where rownum <= 20000";

        static DbConnection CreateConnection()
        {
            return new OracleConnection("Data Source=devbnk2;User ID=pcb_1209;Password=devbnk2");
        }

        public static void Case3()
        {
            var map = new Map<Customer>()
                .Property(m => m.Id, "csuniq")
                .Property(m => m.Name, "csname")
                .Property(m => m.EGN, "egn_ekpou")
                .Collection(m => m.Addresses, (address) =>
                {
                    address
                    .Property(a => a.Id, "cauniq")
                    .Property(a => a.Kind, "cakind")
                    .Property(a => a.City, "cacity")
                    .Property(a => a.Zip, "capk")
                    .Object(a => a.Type, (typ) =>
                    {
                        typ
                        .Property(tp => tp.Id, "hivalue")
                        .Property(tp => tp.Description, "hivdescr");
                    });
                })
                .Collection(m => m.Phones, (phone) =>
                {
                    phone
                    .Property(p => p.Id, "cpuniq")
                    .Property(p => p.Kind, "cpkind")
                    .Property(p => p.AreaCode, "cpareacode")
                    .Property(p => p.Number, "cpphone")
                    .Property(p => p.Contact, "cpcontact")
                    .Object(p => p.Type, (typ) =>
                    {
                        typ
                        .Property(tp => tp.Id, "ptid")
                        .Property(tp => tp.Description, "ptdescr");
                    });
                });

            DateTime t; TimeSpan at; int i = 0;
            TimeSpan[] aat = new TimeSpan[4];
            TimeSpan[] bt = new TimeSpan[4];
            TimeSpan[] pt = new TimeSpan[4];
            TimeSpan[] et = new TimeSpan[4];
            int[] ai = new int[4];

            using (var conn = CreateConnection())
            {
                conn.Open();

                Console.WriteLine("first ..."); t = DateTime.Now; i = 0;

                foreach (Customer c in conn.Enumerate(CommandType.Text, commandText, null, map))
                {
                    Console.WriteLine("{0}, {1}, {2}, {3}, {4}", c.Id, c.Name, c.EGN,
                        null == c.Addresses ? -1 : c.Addresses.Count,
                        null == c.Phones ? -1 : c.Phones.Count);

                    if (null != c.Addresses)
                    {
                        Console.WriteLine("Addresses:");
                        foreach (Address a in c.Addresses)
                        {
                            Console.WriteLine("{0}, {1}, {2}, {3}, {4}", a.Id, a.Kind, a.City, a.Zip, null == a.Type ? -1 : 1);
                            if (null != a.Type)
                                Console.WriteLine("{0}, {1}", a.Type.Id, a.Type.Description);
                        }
                    }
                    if (null != c.Phones)
                    {
                        Console.WriteLine("Phones:");
                        foreach (Phone a in c.Phones)
                        {
                            Console.WriteLine("{0}, {1}, {2}, {3}, {4}, {5}", a.Id, a.Kind, a.AreaCode, a.Number, a.Contact, null == a.Type ? -1 : 1);
                            if (null != a.Type)
                                Console.WriteLine("{0}, {1}", a.Type.Id, a.Type.Description);
                        }
                    }
                    ++i;
                }

                at = DateTime.Now.Subtract(t);
                aat[0] = at;
                ai[0] = i;
                bt[0] = Loader.Build;
                pt[0] = Loader.Projection;
                et[0] = Loader.Execution;

                Console.WriteLine("second ...");
                t = DateTime.Now; i = 0;
                foreach (Customer c in conn.Enumerate<Customer>(CommandType.Text, commandText, null, map))
                {
                    ++i;
                }
                at = DateTime.Now.Subtract(t);
                aat[1] = at;
                ai[1] = i;
                bt[1] = Loader.Build;
                pt[1] = Loader.Projection;
                et[1] = Loader.Execution;
                Console.WriteLine("all ...");
            }

            for (i = 0; i < 2; i++)
            {
                Console.WriteLine("Time {0}: {1}", i, aat[i]);
                Console.WriteLine("Rows {0}: {1}", i, ai[i]);
                Console.WriteLine("Avg  {0}: {1}", i, TimeSpan.FromTicks(aat[i].Ticks / ai[i]));
                Console.WriteLine("Build {0}: {1}", i, bt[i]);
                Console.WriteLine("Projection {0}: {1}", i, pt[i]);
                Console.WriteLine("Projection Avg {0}: {1}", i, TimeSpan.FromTicks(pt[i].Ticks / ai[i]));
                Console.WriteLine("Execution {0}: {1}", i, et[i]);
            }
        }

        public static void Case1()
        {
            string[,] commandToObjectMap = new string[,] {
            {"PrimaryKey", "csuniq"},
            {"Id", "csuniq"},
            {"Name", "csname"},
            {"EGN", "egn_ekpou"},
            {"Addresses.PrimaryKey", "cauniq"},
            {"Addresses.Id", "cauniq"},
            {"Addresses.Kind", "cakind"},
            {"Addresses.City", "cacity"},
            {"Addresses.Zip", "capk"},
            {"Addresses.Type.PrimaryKey", "hivalue"},
            {"Addresses.Type.Id", "hivalue"},
            {"Addresses.Type.Description", "hivdescr"},
            {"Phones.PrimaryKey", "cpuniq"},
            {"Phones.Id", "cpuniq"},
            {"Phones.Kind", "cpkind"},
            {"Phones.AreaCode", "cpareacode"},
            {"Phones.Number", "cpphone"},
            {"Phones.Contact", "cpcontact"},
            {"Phones.Type.Id", "ptid"},
            {"Phones.Type.Description", "ptDescr"},
        };

            DateTime t; TimeSpan at; int i = 0;
            TimeSpan[] aat = new TimeSpan[4];
            int[] ai = new int[4];

            using (var conn = CreateConnection())
            {
                conn.Open();

                Console.WriteLine("first ..."); t = DateTime.Now; i = 0;

                foreach (Customer c in conn.Enumerate<Customer>(CommandType.Text, commandText, null, commandToObjectMap))
                {
                    Console.WriteLine("{0}, {1}, {2}, {3}, {4}", c.Id, c.Name, c.EGN,
                        null == c.Addresses ? -1 : c.Addresses.Count,
                        null == c.Phones ? -1 : c.Phones.Count);

                    if (null != c.Addresses)
                    {
                        Console.WriteLine("Addresses:");
                        foreach (Address a in c.Addresses)
                        {
                            Console.WriteLine("{0}, {1}, {2}, {3}, {4}", a.Id, a.Kind, a.City, a.Zip, null == a.Type ? -1 : 1);
                            if (null != a.Type)
                                Console.WriteLine("{0}, {1}", a.Type.Id, a.Type.Description);
                        }
                    }
                    if (null != c.Phones)
                    {
                        Console.WriteLine("Phones:");
                        foreach (Phone a in c.Phones)
                        {
                            Console.WriteLine("{0}, {1}, {2}, {3}, {4}, {5}", a.Id, a.Kind, a.AreaCode, a.Number, a.Contact, null == a.Type ? -1 : 1);
                            if (null != a.Type)
                                Console.WriteLine("{0}, {1}", a.Type.Id, a.Type.Description);
                        }
                    }
                    ++i;
                }

                at = DateTime.Now.Subtract(t);
                aat[0] = at;
                ai[0] = i;

                Console.WriteLine("second ...");
                t = DateTime.Now; i = 0;
                foreach (Customer c in conn.Enumerate<Customer>(CommandType.Text, commandText, null, commandToObjectMap))
                {
                    ++i;
                }
                at = DateTime.Now.Subtract(t);
                aat[1] = at;
                ai[1] = i;
                Console.WriteLine("all ...");
            }

            for (i = 0; i < 2; i++)
            {
                Console.WriteLine("Time {0}: {1}", i, aat[i]);
                Console.WriteLine("Rows {0}: {1}", i, ai[i]);
            }
        }

        public static void Case2()
        {
            string[,] commandToObjectMap2 = new string[,] {
            {"PrimaryKey", "csuniq"},
            {"Id", "csuniq"},
            {"Name", "csname"},
            {"EGN", "egn_ekpou"},
            {"Addresses.PrimaryKey", "cauniq"},
            {"Addresses.Id", "cauniq"},
            {"Addresses.Kind", "cakind"},
            {"Addresses.City", "cacity"},
            {"Addresses.Zip", "capk"},
            {"Addresses.Type:1.PrimaryKey", "hivalue"},
            {"Addresses.Type:1.Id", "hivalue"},
            {"Addresses.Type:1.Description", "hivdescr"},
            {"Phones.PrimaryKey", "cpuniq"},
            {"Phones.Id", "cpuniq"},
            {"Phones.Kind", "cpkind"},
            {"Phones.AreaCode", "cpareacode"},
            {"Phones.Number", "cpphone"},
            {"Phones.Contact", "cpcontact"},
            {"Phones.Type:1.PrimaryKey", "ptid"},
            {"Phones.Type:1.Id", "ptid"},
            {"Phones.Type:1.Description", "ptDescr"},
        };

            DateTime t; TimeSpan at; int i = 0;
            TimeSpan[] aat = new TimeSpan[4];
            int[] ai = new int[4];

            using (var conn = CreateConnection())
            {
                conn.Open();

                Console.WriteLine("first ..."); t = DateTime.Now; i = 0;

                foreach (var c in conn.Enumerate<dynamic>(CommandType.Text, commandText, null, commandToObjectMap2))
                {
                    Console.WriteLine("{0}, {1}, {2}, {3}, {4}", c.Id, c.Name, c.EGN,
                        null == c.Addresses ? -1 : c.Addresses.Count,
                        null == c.Phones ? -1 : c.Phones.Count);

                    if (null != c.Addresses)
                    {
                        Console.WriteLine("Addresses:");
                        foreach (var a in c.Addresses)
                        {
                            Console.WriteLine("{0}, {1}, {2}, {3}, {4}", a.Id, a.Kind, a.City, a.Zip, null == a.Type ? -1 : 1);
                            if (null != a.Type)
                                Console.WriteLine("{0}, {1}", a.Type.Id, a.Type.Description);
                        }
                    }
                    if (null != c.Phones)
                    {
                        Console.WriteLine("Phones:");
                        foreach (var a in c.Phones)
                        {
                            Console.WriteLine("{0}, {1}, {2}, {3}, {4}, {5}", a.Id, a.Kind, a.AreaCode, a.Number, a.Contact, null == a.Type ? -1 : 1);
                            if (null != a.Type)
                                Console.WriteLine("{0}, {1}", a.Type.Id, a.Type.Description);
                        }
                    }
                    ++i;
                }

                at = DateTime.Now.Subtract(t);
                aat[0] = at;
                ai[0] = i;

                Console.WriteLine("second ...");
                t = DateTime.Now; i = 0;
                foreach (var c in conn.Enumerate<dynamic>(CommandType.Text, commandText, null, commandToObjectMap2))
                {
                    ++i;
                }
                at = DateTime.Now.Subtract(t);
                aat[1] = at;
                ai[1] = i;
                Console.WriteLine("all ...");
            }

            for (i = 0; i < 2; i++)
            {
                Console.WriteLine("Time {0}: {1}", i, aat[i]);
                Console.WriteLine("Rows {0}: {1}", i, ai[i]);
            }
        }
    }
}