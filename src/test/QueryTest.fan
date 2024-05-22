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
             ((rec.paths @> @x1::text[]) and ((rec.refs is null) or (not (rec.refs @> @x2::jsonb))))
           );",
        Str:Obj[
          "x0":"{\"haven\"}",
          "x1":"{\"id\"}",
          "x2":"{\"id\":\"z0\"}",
        ]))

    //-----------------
    // Uri

    doSelect(
      Filter("haven and b == `https://project-haystack.org/`"),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             (rec.uris @> @x1::jsonb)
           );",
        Str:Obj[
          "x0":"{\"haven\"}",
          "x1":"{\"b\":\"https://project-haystack.org/\"}",
        ]))

    doSelect(
      Filter("haven and b != `https://project-haystack.org/`"),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             ((rec.paths @> @x1::text[]) and ((rec.uris is null) or (not (rec.uris @> @x2::jsonb))))
           );",
        Str:Obj[
          "x0":"{\"haven\"}",
          "x1":"{\"b\"}",
          "x2":"{\"b\":\"https://project-haystack.org/\"}",
        ]))

    //-----------------
    // Strs

    ["str":"str", "nest->bar":"nest.bar"].each | dotted, arrows |
    {
      // ==
      doSelect(
        Filter("haven and $arrows == \"y\""),
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
            "x1":"{\"$dotted\":\"y\"}",
          ]))

      // !=
      doSelect(
        Filter("haven and $arrows != \"y\""),
        Query(
          "select rec.brio from rec
           where
             (
               (rec.paths @> @x0::text[])
               and
               ((rec.paths @> @x1::text[]) and ((rec.strs is null) or (not (rec.strs @> @x2::jsonb))))
             );",
          Str:Obj[
            "x0":"{\"haven\"}",
            "x1":"{\"$dotted\"}",
            "x2":"{\"$dotted\":\"y\"}",
          ]))

      // cmp
      ["<", "<=", ">", ">="].each |op|
      {
        doSelect(
          Filter("haven and $arrows $op \"y\""),
          Query(
            "select rec.brio from rec
             where
               (
                 (rec.paths @> @x0::text[])
                 and
                 ((rec.paths @> @x1::text[]) and ((rec.strs ->> @x2) $op @x3))
               );",
            Str:Obj[
              "x0":"{\"haven\"}",
              "x1":"{\"$dotted\"}",
              "x2":dotted,
              "x3":"y",
            ]))
      }
    }

    //-----------------
    // Numbers

    [
      n(2):      null,
      n(2,"°F"): "\"\\u00b0F\"",
      n(2,"m"):  "\"m\"",
    ].each | uparam, num |
    {
      // ==
      doSelect(
        Filter("haven and num == $num"),
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
            "x1":"{\"num\":${num.toFloat}}",
            "x2":"{\"num\":$uparam}",
          ]))

      // !=
      doSelect(
        Filter("haven and num != $num"),
        Query(
          "select rec.brio from rec
           where
             (
               (rec.paths @> @x0::text[])
               and
               ((rec.paths @> @x1::text[]) and ((rec.nums is null) or (not ((rec.nums @> @x2::jsonb) and (rec.units @> @x3::jsonb)))))
             );",
          Str:Obj[
            "x0":"{\"haven\"}",
            "x1":"{\"num\"}",
            "x2":"{\"num\":${num.toFloat}}",
            "x3":"{\"num\":$uparam}",
          ]))

      // cmp
      ["<", "<=", ">", ">="].each |op|
      {
        doSelect(
          Filter("haven and num $op $num"),
          Query(
            "select rec.brio from rec
             where
               (
                 (rec.paths @> @x0::text[])
                 and
                 ((rec.paths @> @x1::text[]) and (((rec.nums -> @x2)::real) $op @x3) and (rec.units @> @x4::jsonb))
               );",
            Str:Obj[
              "x0":"{\"haven\"}",
              "x1":"{\"num\"}",
              "x2":"num",
              "x3":num.toFloat,
              "x4":"{\"num\":$uparam}",
            ]))
      }
    }

    //-----------------
    // Bools

    // ==
    doSelect(
      Filter("haven and bool == true"),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             (rec.bools @> @x1::jsonb)
           );",
        Str:Obj[
          "x0":"{\"haven\"}",
          "x1":"{\"bool\":true}",
        ]))

    // !=
    doSelect(
      Filter("haven and bool != true"),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             ((rec.paths @> @x1::text[]) and ((rec.bools is null) or (not (rec.bools @> @x2::jsonb))))
           );",
        Str:Obj[
          "x0":"{\"haven\"}",
          "x1":"{\"bool\"}",
          "x2":"{\"bool\":true}",
        ]))

    // cmp
    ["<", "<=", ">", ">="].each |op|
    {
      doSelect(
        Filter("haven and bool $op true"),
        Query(
          "select rec.brio from rec
           where
             (
               (rec.paths @> @x0::text[])
               and
               ((rec.paths @> @x1::text[]) and (((rec.bools -> @x2)::boolean) $op @x3))
             );",
          Str:Obj[
            "x0":"{\"haven\"}",
            "x1":"{\"bool\"}",
            "x2":"bool",
            "x3":true,
          ]))

    }

    //-----------------
    // Dates

    ["foo":"foo", "quux->date":"quux.date"].each | dotted, arrows |
    {
      // ==
      doSelect(
        Filter("haven and $arrows == 2021-03-22"),
        Query(
          "select rec.brio from rec
           where
             (
               (rec.paths @> @x0::text[])
               and
               (rec.dates @> @x1::jsonb)
             );",
          Str:Obj[
            "x0":"{\"haven\"}",
            "x1":"{\"$dotted\":\"2021-03-22\"}",
          ]))

      // !=
      doSelect(
        Filter("haven and $arrows != 2021-03-22"),
        Query(
          "select rec.brio from rec
           where
             (
               (rec.paths @> @x0::text[])
               and
               ((rec.paths @> @x1::text[]) and ((rec.dates is null) or (not (rec.dates @> @x2::jsonb))))
             );",
          Str:Obj[
            "x0":"{\"haven\"}",
            "x1":"{\"$dotted\"}",
            "x2":"{\"$dotted\":\"2021-03-22\"}",
          ]))

      // cmp
      ["<", "<=", ">", ">="].each |op|
      {
        doSelect(
          Filter("haven and $arrows $op 2021-03-22"),
          Query(
            "select rec.brio from rec
             where
               (
                 (rec.paths @> @x0::text[])
                 and
                 ((rec.paths @> @x1::text[]) and ((rec.dates ->> @x2) $op @x3))
               );",
            Str:Obj[
              "x0":"{\"haven\"}",
              "x1":"{\"$dotted\"}",
              "x2":dotted,
              "x3":"2021-03-22",
            ]))
      }
    }

    //-----------------
    // Times

    ["foo":"foo", "quux->time":"quux.time"].each | dotted, arrows |
    {
      // ==
      doSelect(
        Filter("haven and $arrows == 17:19:23"),
        Query(
          "select rec.brio from rec
           where
             (
               (rec.paths @> @x0::text[])
               and
               (rec.times @> @x1::jsonb)
             );",
          Str:Obj[
            "x0":"{\"haven\"}",
            "x1":"{\"$dotted\":\"17:19:23\"}",
          ]))

      // !=
      doSelect(
        Filter("haven and $arrows != 17:19:23"),
        Query(
          "select rec.brio from rec
           where
             (
               (rec.paths @> @x0::text[])
               and
               ((rec.paths @> @x1::text[]) and ((rec.times is null) or (not (rec.times @> @x2::jsonb))))
             );",
          Str:Obj[
            "x0":"{\"haven\"}",
            "x1":"{\"$dotted\"}",
            "x2":"{\"$dotted\":\"17:19:23\"}",
          ]))

      // cmp
      ["<", "<=", ">", ">="].each |op|
      {
        doSelect(
          Filter("haven and $arrows $op 17:19:23"),
          Query(
            "select rec.brio from rec
             where
               (
                 (rec.paths @> @x0::text[])
                 and
                 ((rec.paths @> @x1::text[]) and ((rec.times ->> @x2) $op @x3))
               );",
            Str:Obj[
              "x0":"{\"haven\"}",
              "x1":"{\"$dotted\"}",
              "x2":dotted,
              "x3":"17:19:23",
            ]))
      }
    }

    //-----------------
    // DateTimes

    ts := DateTime.fromIso("2021-03-22T17:19:23.000-04:00")

    // ==
    doSelect(
      Filter.has("haven").and(Filter.eq("quux", ts)),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             (rec.dateTimes @> @x1::jsonb)
           );",
        Str:Obj[
          "x0":"{\"haven\"}",
          "x1":"{\"quux\":669763163000}",
        ]))

    // !=
    doSelect(
      Filter.has("haven").and(Filter.ne("quux", ts)),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             ((rec.paths @> @x1::text[]) and ((rec.dateTimes is null) or (not (rec.dateTimes @> @x2::jsonb))))
           );",
        Str:Obj[
          "x0":"{\"haven\"}",
          "x1":"{\"quux\"}",
          "x2":"{\"quux\":669763163000}",
        ]))

    // cmp
    [
      "<":  Filter.lt("quux", ts),
      "<=": Filter.le("quux", ts),
      ">":  Filter.gt("quux", ts),
      ">=": Filter.ge("quux", ts)
    ].each |f, op|
    {
      doSelect(
        Filter.has("haven").and(f),
        Query(
          "select rec.brio from rec
           where
             (
               (rec.paths @> @x0::text[])
               and
               ((rec.paths @> @x1::text[]) and (((rec.dateTimes -> @x2)::bigint) $op @x3))
             );",
          Str:Obj[
            "x0":"{\"haven\"}",
            "x1":"{\"quux\"}",
            "x2":"quux",
            "x3":669763163000,
          ]))
    }

    //echo("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
    //filter := Filter("haven and num < 2")
    //query := Query(filter)
    //echo(filter)
    //dumpQuery(query)
    //echo()

    //echo("Raw:")
    //echo("explain (analyze true, verbose true, buffers true) ")
    //raw := rawSql(query)
    //raw = raw.replace("rec.brio", "rec.id")
    //echo(raw)
    //echo()

    //expected := testData.filter(filter)
    //echo("expected ${expected.size} rows")
    //echo(expected.map |Dict v->Ref| { v.id })

    //found := haven.select(query)
    //echo("found ${found.size} rows")
    //echo(found.map |Dict v->Ref| { v.id })

    echo("==============================================================")
  }

  Void testMismatchedType()
  {
    doSelect(
      Filter("haven and str == 2"),
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
          "x1":"{\"str\":2.0}",
          "x2":"{\"str\":null}",
        ]))
  }

  Void testAlpha()
  {
    echo("==============================================================")

    doSelect(
      Filter("ahu"),
      Query(
        "select rec.brio from rec
         where
           (rec.paths @> @x0::text[]);",
        Str:Obj["x0": "{\"ahu\"}"]))

    doSelect(
      Filter("chilledWaterRef->chilled"),
      Query(
        "select rec.brio from rec
           inner join path_ref p1 on p1.source = rec.id
           inner join rec      r1 on r1.id     = p1.target
         where
           ((p1.path_ = @x0) and (r1.paths @> @x1::text[]));",
        Str:Obj[
          "x0":"chilledWaterRef",
          "x1":"{\"chilled\"}"]))

    doSelect(
      Filter("ahu and elec"),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.paths @> @x0::text[])
             and
             (rec.paths @> @x1::text[])
           );",
        Str:Obj[
          "x0":"{\"ahu\"}",
          "x1":"{\"elec\"}"]))

    doSelect(
      Filter("chilled and pump and sensor and equipRef->siteRef->site"),
      Query(
        "select rec.brio from rec
           inner join path_ref p1 on p1.source = rec.id
           inner join rec      r1 on r1.id     = p1.target
           inner join path_ref p2 on p2.source = r1.id
           inner join rec      r2 on r2.id     = p2.target
         where
           (
             (
               (
                 (rec.paths @> @x0::text[])
                 and
                 ((p1.path_ = @x1) and (p2.path_ = @x2) and (r2.paths @> @x3::text[]))
               )
               and
               (rec.paths @> @x4::text[])
             )
             and
             (rec.paths @> @x5::text[])
           );",
        Str:Obj[
          "x0":"{\"chilled\"}",
          "x1":"equipRef",
          "x2":"siteRef",
          "x3":"{\"site\"}",
          "x4":"{\"pump\"}",
          "x5":"{\"sensor\"}"]))

    doSelect(
      Filter("custom->description == \"Clg_Valve_Cmd\""),
      Query(
        "select rec.brio from rec
         where
           (rec.strs @> @x0::jsonb);",
        Str:Obj[
          "x0": """{"custom.description":"Clg_Valve_Cmd"}"""
        ]))

    doSelect(
      Filter("dis == \"Alpha Airside AHU-4\""),
      Query(
        "select rec.brio from rec
         where
           (rec.strs @> @x0::jsonb);",
        Str:Obj[
          "x0":"{\"dis\":\"Alpha Airside AHU-4\"}"]))

    doSelect(
      Filter("geoElevation == 2956m"),
      Query(
        "select rec.brio from rec
         where
           ((rec.nums @> @x0::jsonb) and (rec.units @> @x1::jsonb));",
        Str:Obj[
          "x0":"{\"geoElevation\":2956.0}",
          "x1":"{\"geoElevation\":\"m\"}"
        ]))

    doSelect(
      Filter("equipRef == @a-0001"),
      Query(
        "select rec.brio from rec
         where
           (rec.refs @> @x0::jsonb);",
        Str:Obj[
          "x0":"{\"equipRef\":\"a-0001\"}"
        ]))

    doSelect(
      Filter("id->area"),
      Query(
        "select rec.brio from rec
           inner join path_ref p1 on p1.source = rec.id
           inner join rec      r1 on r1.id     = p1.target
         where
           ((p1.path_ = @x0) and (r1.paths @> @x1::text[]));",
        Str:Obj[
          "x0":"id",
          "x1":"{\"area\"}"]))
  }

  Void testSeqScan()
  {
    echo("==============================================================")

    doSelect(
      Filter("not point"),
      Query(
        "select rec.brio from rec
         where
           (not (rec.paths @> @x0::text[]));",
        Str:Obj[
          "x0":"{\"point\"}"]),
        true)
  }

  Void testNiagara()
  {
    echo("==============================================================")

    doSelect(
      Filter("facets->min"),
      Query(
        "select rec.brio from rec
         where
           (rec.paths @> @x0::text[]);",
        Str:Obj["x0": "{\"facets.min\"}"]))

    doSelect(
      Filter("links->in4->fromRef->meta->inA->flags->linkTarget"),
      Query(
        "select rec.brio from rec
           inner join path_ref p1 on p1.source = rec.id
           inner join rec      r1 on r1.id     = p1.target
         where
           ((p1.path_ = @x0) and (r1.paths @> @x1::text[]));",
        Str:Obj[
          "x0":"links.in4.fromRef",
          "x1":"{\"meta.inA.flags.linkTarget\"}"]))

    doSelect(
      Filter("links->in4->fromRef->meta->inA->flags->linkTarget and parentRef->parentRef->slotPath"),
      Query(
        "select rec.brio from rec
           inner join path_ref p1 on p1.source = rec.id
           inner join rec      r1 on r1.id     = p1.target
           inner join path_ref p2 on p2.source = r1.id
           inner join rec      r2 on r2.id     = p2.target
           inner join path_ref p3 on p3.source = r2.id
           inner join rec      r3 on r3.id     = p3.target
         where
           (
             ((p1.path_ = @x0) and (r1.paths @> @x1::text[]))
             and
             ((p2.path_ = @x2) and (p3.path_ = @x3) and (r3.paths @> @x4::text[]))
           );",
        Str:Obj[
          "x0":"links.in4.fromRef",
          "x1":"{\"meta.inA.flags.linkTarget\"}",
          "x2":"parentRef",
          "x3":"parentRef",
          "x4":"{\"slotPath\"}"]))

    doSelect(
      Filter("parentRef->parentRef->slotPath == \"slot:/AHUSystem/vavs\""),
      Query(
        "select rec.brio from rec
           inner join path_ref p1 on p1.source = rec.id
           inner join rec      r1 on r1.id     = p1.target
           inner join path_ref p2 on p2.source = r1.id
           inner join rec      r2 on r2.id     = p2.target
         where
           ((p1.path_ = @x0) and (p2.path_ = @x1) and (r2.strs @> @x2::jsonb));",
        Str:Obj[
          "x0":"parentRef",
          "x1":"parentRef",
          "x2":"{\"slotPath\":\"slot:/AHUSystem/vavs\"}"]))

    doSelect(
      Filter("facets->precision == 1"),
      Query(
        "select rec.brio from rec
         where
           ((rec.nums @> @x0::jsonb) and (rec.units @> @x1::jsonb));",
        Str:Obj[
          "x0":"{\"facets.precision\":1.0}",
          "x1":"{\"facets.precision\":null}",
        ]))
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
    echo("expected ${expected.size} rows")
    //echo(expected.map |Dict v->Ref| { v.id })

    // Construct the Query and make sure it matches the expected query
    query := Query.fromFilter(filter)
    echo("--------------")
    dumpQuery(expectedQuery)
    echo("--------------")
    dumpQuery(query)
    echo("--------------")
    echo("sql eq ${expectedQuery.sql == query.sql}")
    echo("params eq ${expectedQuery.params == query.params}")
    echo("--------------")
    verifyEq(expectedQuery, query)

    // Explain the Query's raw sql to make sure its not a sequential scan
    explained := explain(rawSql(query))
    echo("explain (analyze true, verbose true, buffers true) ")
    echo(rawSql(query))
    seq := isSeqScan(explained)
    if (seq) echo("************ SEQUENTIAL ************")
    if (!allowSequential)
      verifyFalse(seq)
    explained.each |s| {
      if (s.startsWith("Execution Time:"))
        echo(s)
    }

    // Perfom the query in the database
    found := haven.select(query)
    echo("found ${found.size} rows")
    //echo(found.map |Dict v->Ref| { v.id })

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

  Void testDateTime()
  {
    a := DateTime.fromIso("2001-02-01T23:00:00-01:00")
    b := DateTime.fromIso("2001-02-02T01:00:00+01:00")
    verifyEq(a, b)
    verifyFalse(a.date == b.date)
    verifyFalse(a.time == b.time)
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

  private static Void dumpQuery(Query query)
  {
    echo("'${query.sql}'")
    query.params.each | val, key |
    {
      echo("$key: ${val.typeof} $val")
    }
  }

  private static Ref ref(Str str) { Ref.fromStr(str) }

  private static Number n(Num val, Str? unit := null)
  {
    Number.makeNum(val, unit == null ? null : Unit.fromStr(unit))
  }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  private TestData testData := TestData()
  private Haven haven := Haven()
}
