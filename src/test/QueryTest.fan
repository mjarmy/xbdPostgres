//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

**
** QueryTest
**
class QueryTest : Test
{
  override Void setup()
  {
    haven.open(
      "jdbc:postgresql://localhost/postgres",
      "xbd",
      "s3crkEt")
  }

  override Void teardown()
  {
    haven.close()
  }

  Void testSelectById()
  {
    verifyTrue(
      Etc.dictEq(
        haven.selectById(ref("z0")),
        testData.recs[ref("z0")]
      ))

    verifyTrue(haven.selectById(ref("bogus")) == null)
  }

  Void testSelectByIds()
  {
    verifyDictsEq(
      haven.selectByIds(Ref[,]),
      Dict[,])

    verifyDictsEq(
      haven.selectByIds(Ref[
        ref("bogus"),
        ref("z0")
      ]),
      Dict[
        testData.recs[ref("z0")]
      ])

    verifyDictsEq(
      haven.selectByIds(Ref[
        ref("z0"),
        ref("z1"),
        ref("z2"),
        ref("z3")
      ]),
      Dict[
        testData.recs[ref("z0")],
        testData.recs[ref("z1")],
        testData.recs[ref("z2")],
        testData.recs[ref("z3")]
      ])
  }

  Void testSelectHaven()
  {
    echo("==============================================================")

    //-----------------
    // has

    doSelect(
      Filter("haven"),
      Query(
        "select rec.brio from rec
         where
           (rec.paths @> @x0::text[]);",
        Str:Obj[
          "x0": "{\"haven\"}"
        ]))

    doSelect(
      Filter("haven and e"),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             (rec.paths @> @x1::text[])
           );",
        Str:Obj[
          "x0":"{\"e\"}",
          "x1":"{\"haven\"}"
        ]))

    doSelect(
      Filter("haven and (str or num)"),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             (
               (rec.paths @> @x1::text[])
               or
               (rec.paths @> @x2::text[])
             )
           );",
        Str:Obj[
          "x0":"{\"haven\"}",
          "x1":"{\"num\"}",
          "x2":"{\"str\"}"
        ]))

    //-----------------
    // Refs

    doSelect(
      Filter("haven and id == @z0"),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             (rec.refs @> @x1::jsonb)
           );",
        Str:Obj[
          "x0":"{\"haven\"}",
          "x1":"{\"id\":\"z0\"}",
        ]))

    doSelect(
      Filter("haven and id != @z0"),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             ((rec.paths @> @x1::text[]) and (not (rec.refs @> @x2::jsonb)))
           );",
        Str:Obj[
          "x0":"{\"haven\"}",
          "x1":"{\"id\"}",
          "x2":"{\"id\":\"z0\"}",
        ]))


    doSelect(
      Filter("haven and id != @z0"),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             ((rec.paths @> @x1::text[]) and (not (rec.refs @> @x2::jsonb)))
           );",
        Str:Obj[
          "x0":"{\"haven\"}",
          "x1":"{\"id\"}",
          "x2":"{\"id\":\"z0\"}",
        ]))

    //-----------------
    // Strs

    doSelect(
      Filter("haven and str == \"y\""),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             (rec.strs @> @x1::jsonb)
           );",
        Str:Obj[
          "x0":"{\"haven\"}",
          "x1":"{\"str\":\"y\"}",
        ]))

    doSelect(
      Filter("haven and nest->bar == \"y\""),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             (rec.strs @> @x1::jsonb)
           );",
        Str:Obj[
          "x0":"{\"haven\"}",
          "x1":"{\"nest.bar\":\"y\"}",
        ]))

    doSelect(
      Filter("haven and str != \"y\""),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             ((rec.paths @> @x1::text[]) and (not (rec.strs @> @x2::jsonb)))
           );",
        Str:Obj[
          "x0":"{\"haven\"}",
          "x1":"{\"str\"}",
          "x2":"{\"str\":\"y\"}",
        ]))

    doSelect(
      Filter("haven and nest->bar < \"y\""),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             ((rec.paths @> @x1::text[]) and ((rec.strs->>@x2)::text < @x3))
           );",
        Str:Obj[
          "x0":"{\"haven\"}",
          "x1":"{\"nest.bar\"}",
          "x2":"nest.bar",
          "x3":"y",
        ]))

    doSelect(
      Filter("haven and str < \"y\""),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             ((rec.paths @> @x1::text[]) and ((rec.strs->>@x2)::text < @x3))
           );",
        Str:Obj[
          "x0":"{\"haven\"}",
          "x1":"{\"str\"}",
          "x2":"str",
          "x3":"y",
        ]))

    doSelect(
      Filter("haven and str <= \"y\""),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             ((rec.paths @> @x1::text[]) and ((rec.strs->>@x2)::text <= @x3))
           );",
        Str:Obj[
          "x0":"{\"haven\"}",
          "x1":"{\"str\"}",
          "x2":"str",
          "x3":"y",
        ]))

    doSelect(
      Filter("haven and str > \"y\""),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             ((rec.paths @> @x1::text[]) and ((rec.strs->>@x2)::text > @x3))
           );",
        Str:Obj[
          "x0":"{\"haven\"}",
          "x1":"{\"str\"}",
          "x2":"str",
          "x3":"y",
        ]))

    doSelect(
      Filter("haven and str >= \"y\""),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             ((rec.paths @> @x1::text[]) and ((rec.strs->>@x2)::text >= @x3))
           );",
        Str:Obj[
          "x0":"{\"haven\"}",
          "x1":"{\"str\"}",
          "x2":"str",
          "x3":"y",
        ]))

    //-----------------
    // Strs

    doSelect(
      Filter("haven and num == 2"),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             ((rec.nums @> @x1::jsonb) and (rec.units @> @x2::jsonb))
           );",
        Str:Obj[
          "x0":"{\"haven\"}",
          "x1":"{\"num\":2.0}",
          "x2":"{\"num\":null}",
        ]))

    doSelect(
      Filter("haven and num != 2"),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             ((rec.paths @> @x1::text[]) and (not ((rec.nums @> @x2::jsonb) and (rec.units @> @x3::jsonb))))
           );",
        Str:Obj[
          "x0":"{\"haven\"}",
          "x1":"{\"num\"}",
          "x2":"{\"num\":2.0}",
          "x3":"{\"num\":null}",
        ]))

    doSelect(
      Filter("haven and num == 2m"),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             ((rec.nums @> @x1::jsonb) and (rec.units @> @x2::jsonb))
           );",
        Str:Obj[
          "x0":"{\"haven\"}",
          "x1":"{\"num\":2.0}",
          "x2":"{\"num\":\"m\"}",
        ]))

    doSelect(
      Filter("haven and num != 2m"),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             ((rec.paths @> @x1::text[]) and (not ((rec.nums @> @x2::jsonb) and (rec.units @> @x3::jsonb))))
           );",
        Str:Obj[
          "x0":"{\"haven\"}",
          "x1":"{\"num\"}",
          "x2":"{\"num\":2.0}",
          "x3":"{\"num\":\"m\"}",
        ]))

    doSelect(
      Filter("haven and num == 2F"),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             ((rec.nums @> @x1::jsonb) and (rec.units @> @x2::jsonb))
           );",
        Str:Obj[
          "x0":"{\"haven\"}",
          "x1":"{\"num\":2.0}",
          "x2":"{\"num\":\"F\"}",
        ]))

    doSelect(
      Filter("haven and num != 2F"),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             ((rec.paths @> @x1::text[]) and (not ((rec.nums @> @x2::jsonb) and (rec.units @> @x3::jsonb))))
           );",
        Str:Obj[
          "x0":"{\"haven\"}",
          "x1":"{\"num\"}",
          "x2":"{\"num\":2.0}",
          "x3":"{\"num\":\"F\"}",
        ]))

//    doSelect(
//      Filter("haven and str != \"y\""),
//      Query(
//        "select rec.brio from rec
//         where
//           (
//             (rec.paths @> @x0::text[])
//             and
//             ((rec.paths @> @x1::text[]) and (not (rec.strs @> @x2::jsonb)))
//           );",
//        Str:Obj[
//          "x0":"{\"haven\"}",
//          "x1":"{\"str\"}",
//          "x2":"{\"str\":\"y\"}",
//        ]))

//    echo("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
//    filter := Filter("haven and (str or num)")
//    query := Query(filter)
//    echo(filter)
//    echo(query)
//    echo()
//
//    echo("Raw:")
//    echo("explain (analyze true, verbose true, buffers true) ")
//    raw := rawSql(query)
//    raw = raw.replace("rec.brio", "rec.id")
//    echo(raw)
//    echo()
//
//    found := haven.select(query)
//    echo("found ${found.size} rows")
//    echo(found.map |Dict v->Ref| { v.id })

    echo("==============================================================")
  }

  private Void doSelect(
    Filter filter,
    Query expectedQuery,
    Bool allowSequential := false)
  {
    echo("--------------------------------------------------------------")
    echo(filter)

    // Fetch the expected test data
    expected := testData.filter(filter)
    //echo("expected ${expected.size} rows")
    //echo(expected.map |Dict v->Ref| { v.id })

    // Construct the Query and make sure it matches the expected query
    query := Query.fromFilter(filter)
    echo()
    echo(query)
    echo("--------------")
    verifyEq(query, expectedQuery)

//    // Explain the Query's raw sql to make sure its not a sequential scan
//    explained := explain(rawSql(query))
//    //echo("explain (analyze true, verbose true, buffers true) ")
//    //echo(rawSql(query))
//    seq := isSeqScan(explained)
//    if (seq) echo("************ SEQUENTIAL ************")
//    if (!allowSequential)
//      verifyFalse(seq)
//    explained.each |s| {
//      if (s.startsWith("Execution Time:"))
//        echo(s)
//    }

    // Perfom the query in the database
    found := haven.select(query)
    echo("found ${found.size} rows")
    echo(found.map |Dict v->Ref| { v.id })

    // Make sure the results match the test data
    verifyDictsEq(expected, found)
  }

  Void testDottedPaths()
  {
    verifyEq(
      QueryBuilder.dottedPaths(Filter("ahu").argA),
      ["ahu"])

    verifyEq(
      QueryBuilder.dottedPaths(Filter("facets->min").argA),
      ["facets.min"])

    verifyEq(
      QueryBuilder.dottedPaths(Filter("chilledWaterRef->chilled").argA),
      ["chilledWaterRef", "chilled"])

    verifyEq(
      QueryBuilder.dottedPaths(Filter("fooOf->barRef").argA),
      ["fooOf", "barRef"])

    verifyEq(
      QueryBuilder.dottedPaths(Filter("links->in4->fromRef->meta->inA->flags->linkTarget").argA),
      ["links.in4.fromRef", "meta.inA.flags.linkTarget"])

    verifyEq(
      QueryBuilder.dottedPaths(Filter("equipRef->siteRef->area").argA),
      ["equipRef", "siteRef", "area"])
  }

//  Void testExplain()
//  {
//    exp := haven.explain("select * from rec")
//    //exp.each |s| { echo(s) }
//    verifyTrue(isSeqScan(exp))
//
//    exp = haven.explain(
//      "select * from rec
//         inner join path_ref p1 on p1.source = rec.id
//         inner join rec     r1 on r1.id     = p1.target
//       where
//         (p1.path_ = 'chilledWaterRef') and
//         (r1.paths @> '{\"chilled\"}'::text[])")
//    //exp.each |s| { echo(s) }
//    verifyFalse(isSeqScan(exp))
//  }

  **
  ** Explain a select
  **
  Str[] explain(Str rawSql)
  {
    res := Str[,]

    stmt := haven.conn.sql(
        "explain (analyze true, verbose true, buffers true) " +
        rawSql)
    stmt.query().each |row|
    {
      col := row.col("QUERY PLAN")
      res.add(row[col])
    }
    stmt.close

    return res
  }

  // This isn't actually reliable, its just a quick and dirty approach for
  // debugging purposes
  private static Str rawSql(Query query)
  {
    Str s := query.sql
    query.params.each |v,k|
    {
      s = s.replace("@" + k, "'$v'")
    }
    return s
  }

  private static Bool isSeqScan(Str[] explain)
  {
    res := false
    explain.each |s| {
      if (s.contains("Seq Scan"))
        res = true;
    }
    return res;
  }

  private Void verifyDictsEq(Dict[] a, Dict[] b)
  {
    verifyEq(a.size, a.size)

    a.sort |Dict x, Dict y->Int| { return x.id.id <=> y.id.id }
    b.sort |Dict x, Dict y->Int| { return x.id.id <=> y.id.id }

    a.each |dict, i|
    {
      verifyTrue(Etc.dictEq(a[i], b[i]))
    }
  }

  private static Ref ref(Str str) { Ref.fromStr(str) }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  private TestData testData := TestData()
  private Haven haven := Haven()
}
