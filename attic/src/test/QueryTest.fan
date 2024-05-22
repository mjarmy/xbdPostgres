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

  Void testQuery()
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
      Filter("facets->min"),
      Query(
        "select rec.brio from rec
         where
           (rec.paths @> @x0::text[]);",
        Str:Obj["x0": "{\"facets.min\"}"]))

    doSelect(
      Filter("chilledWaterRef->chilled"),
      Query(
        "select rec.brio from rec
           inner join path_ref p1 on p1.source = rec.id
           inner join rec     r1 on r1.id     = p1.target
         where
           ((p1.path_ = @x0) and (r1.paths @> @x1::text[]));",
        Str:Obj[
          "x0":"chilledWaterRef",
          "x1":"{\"chilled\"}"]))

    doSelect(
      Filter("links->in4->fromRef->meta->inA->flags->linkTarget"),
      Query(
        "select rec.brio from rec
           inner join path_ref p1 on p1.source = rec.id
           inner join rec     r1 on r1.id     = p1.target
         where
           ((p1.path_ = @x0) and (r1.paths @> @x1::text[]));",
        Str:Obj[
          "x0":"links.in4.fromRef",
          "x1":"{\"meta.inA.flags.linkTarget\"}"]))

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
           inner join rec     r1 on r1.id     = p1.target
           inner join path_ref p2 on p2.source = r1.id
           inner join rec     r2 on r2.id     = p2.target
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
      Filter("links->in4->fromRef->meta->inA->flags->linkTarget and parentRef->parentRef->slotPath"),
      Query(
        "select rec.brio from rec
           inner join path_ref p1 on p1.source = rec.id
           inner join rec     r1 on r1.id     = p1.target
           inner join path_ref p2 on p2.source = r1.id
           inner join rec     r2 on r2.id     = p2.target
           inner join path_ref p3 on p3.source = r2.id
           inner join rec     r3 on r3.id     = p3.target
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
      Filter("custom->description == \"Clg_Valve_Cmd\""),
      Query(
        "select rec.brio from rec
         where
           (rec.hayson @> @x0::jsonb);",
        Str:Obj[
          "x0":"{\"custom\":{\"description\":\"Clg_Valve_Cmd\"}}"]))

    doSelect(
      Filter("dis == \"Alpha Airside AHU-4\""),
      Query(
        "select rec.brio from rec
         where
           (rec.hayson @> @x0::jsonb);",
        Str:Obj[
          "x0":"{\"dis\":\"Alpha Airside AHU-4\"}"]))

    doSelect(
      Filter("parentRef->parentRef->slotPath == \"slot:/AHUSystem/vavs\""),
      Query(
        "select rec.brio from rec
           inner join path_ref p1 on p1.source = rec.id
           inner join rec     r1 on r1.id     = p1.target
           inner join path_ref p2 on p2.source = r1.id
           inner join rec     r2 on r2.id     = p2.target
         where
           ((p1.path_ = @x0) and (p2.path_ = @x1) and (r2.hayson @> @x2::jsonb));",
        Str:Obj[
          "x0":"parentRef",
          "x1":"parentRef",
          "x2":"{\"slotPath\":\"slot:/AHUSystem/vavs\"}"]))

    doSelect(
      Filter("geoElevation == 2956m"),
      Query(
        "select rec.brio from rec
         where
           (rec.hayson @> @x0::jsonb);",
        Str:Obj[
          "x0":"{\"area\":151455}"]))

    doSelect(
      Filter("facets->precision == 1"),
      Query(
        "select rec.brio from rec
         where
           (rec.hayson @> @x0::jsonb);",
        Str:Obj[
          "x0":"{\"facets\":{\"precision\":1}}"]))

    doSelect(
      Filter("equipRef == @a-0001"),
      Query(
        "select rec.brio from rec
         where
           (rec.hayson @> @x0::jsonb);",
        Str:Obj[
          "x0":"{\"equipRef\":{\"_kind\":\"ref\", \"val\":\"a-0001\"}}"]))

    doSelect(
      Filter("id->area"),
      Query(
        "select rec.brio from rec
           inner join path_ref p1 on p1.source = rec.id
           inner join rec     r1 on r1.id     = p1.target
         where
           ((p1.path_ = @x0) and (r1.paths @> @x1::text[]));",
        Str:Obj[
          "x0":"id",
          "x1":"{\"area\"}"]))

    doSelect(
      Filter("not point"),
      Query(
        "select rec.brio from rec
         where
           (not (rec.paths @> @x0::text[]));",
        Str:Obj[
          "x0":"{\"point\"}"]),
        true)

    doSelect(
      Filter("b != 1"),
      Query(
        "select rec.brio from rec
         where
           ((rec.paths @> @x0::text[]) and (not (rec.hayson @> @x1::jsonb)));",
        Str:Obj[
          "x0":"{\"b\"}",
          "x1":"{\"b\":1}"]))

    doSelect(
      Filter("c->d != 1"),
      Query(
        "select rec.brio from rec
         where
           ((rec.paths @> @x0::text[]) and (not (rec.hayson @> @x1::jsonb)));",
        Str:Obj[
          "x0":"{\"c.d\"}",
          "x1":"{\"c\":{\"d\":1}}"]))

    doSelect(
      Filter("a == 1 or b == 2"),
      Query(
        "select rec.brio from rec
         where
           (
             (rec.hayson @> @x0::jsonb)
             or
             (rec.hayson @> @x1::jsonb)
           );",
        Str:Obj[
          "x0":"{\"a\":1}",
          "x1":"{\"b\":2}"]))

    doSelect(
      Filter("e == true"),
      Query(
        "select rec.brio from rec
         where
           (rec.hayson @> @x0::jsonb);",
        Str:Obj[
          "x0":"{\"e\":true}"]))

    doSelect(
      Filter("e != true"),
      Query(
        "select rec.brio from rec
         where
           ((rec.paths @> @x0::text[]) and (not (rec.hayson @> @x1::jsonb)));",
        Str:Obj[
          "x0":"{\"e\"}",
          "x1":"{\"e\":true}"]))

    doSelect(
      Filter("f == `https://project-haystack.org`"),
      Query(
        "select rec.brio from rec
         where
           (rec.hayson @> @x0::jsonb);",
        Str:Obj[
          "x0":"{\"f\":{\"_kind\":\"uri\", \"val\":\"https://project-haystack.org/\"}}"]))

    doSelect(
      Filter("f != `https://project-haystack.org`"),
      Query(
        "select rec.brio from rec
         where
           ((rec.paths @> @x0::text[]) and (not (rec.hayson @> @x1::jsonb)));",
        Str:Obj[
          "x0":"{\"f\"}",
          "x1":"{\"f\":{\"_kind\":\"uri\", \"val\":\"https://project-haystack.org/\"}}"]))

    doSelect(
      Filter("x == 2021-03-22"),
      Query(
        "select rec.brio from rec
         where
           (rec.hayson @> @x0::jsonb);",
        Str:Obj[
          "x0":"{\"x\":{\"_kind\":\"date\", \"val\":\"2021-03-22\"}}"]))

    doSelect(
      Filter("y == 17:19:23"),
      Query(
        "select rec.brio from rec
         where
           (rec.hayson @> @x0::jsonb);",
        Str:Obj[
          "x0":"{\"y\":{\"_kind\":\"time\", \"val\":\"17:19:23\"}}"]))

    dt := DateTime.fromIso("2021-03-22T13:57:00.381-04:00")
    millis := Duration(dt.ticks).toMillis
    doSelect(
      Filter.eq("z", dt),
      Query(
        "select rec.brio from rec
         where
           (rec.hayson @> @x0::jsonb);",
        Str:Obj[
          "x0":"{\"z\":{\"_kind\":\"dateTime\", \"millis\":$millis}}"]))

    doSelect(
      Filter.ne("z", dt),
      Query(
        "select rec.brio from rec
         where
           ((rec.paths @> @x0::text[]) and (not (rec.hayson @> @x1::jsonb)));",
        Str:Obj[
          "x0":"{\"z\"}",
          "x1":"{\"z\":{\"_kind\":\"dateTime\", \"millis\":$millis}}"]))

    echo(Filter("geoElevation == 2956"))

    //echo("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
    //filter := Filter("a == 1 or c == 1")
    //expected := testData.filter(filter)
    //echo("expected ${expected.size} rows")
    ////echo(expected.map |Dict v->Ref| { v.id })
    //query := Query(filter)
    //echo(query)
    //echo("explain (analyze true, verbose true, buffers true) ")
    //echo(rawSql(query))
    //found := haven.select(query)
    //echo("found ${found.size} rows")

    echo("==============================================================")
  }

  Void doSelect(
    Filter filter,
    Query expectedQuery,
    Bool allowSequential := false)
  {
    echo("--------------------------------------------------------------")
    echo(filter)

    // Fetch the expected test data
    expected := testData.filter(filter)
    echo("expected ${expected.size} rows")
    expected.sort |Dict a, Dict b->Int| { return a.id.id <=> b.id.id }
    //echo(expected.map |Dict v->Ref| { v.id })

    // Construct the Query and make sure it matches the expected query
    query := Query.fromFilter(filter)
    echo(query)
    verifyEq(query, expectedQuery)

    // Explain the Query's raw sql to make sure its not a sequential scan
    explained := haven.explain(rawSql(query))
    //echo("explain (analyze true, verbose true, buffers true) ")
    //echo(rawSql(query))
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
    found.sort |Dict a, Dict b->Int| { return a.id.id <=> b.id.id }
    //echo(found.map |Dict v->Ref| { v.id })

    // Make sure the results match the test data
    verifyEq(expected.size, found.size)
    expected.each |ed, i|
    {
      verifyTrue(Etc.dictEq(ed, found[i]));
    }
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

  Void testExplain()
  {
    exp := haven.explain("select * from rec")
    //exp.each |s| { echo(s) }
    verifyTrue(isSeqScan(exp))

    exp = haven.explain(
      "select * from rec
         inner join path_ref p1 on p1.source = rec.id
         inner join rec     r1 on r1.id     = p1.target
       where
         (p1.path_ = 'chilledWaterRef') and
         (r1.paths @> '{\"chilled\"}'::text[])")
    //exp.each |s| { echo(s) }
    verifyFalse(isSeqScan(exp))
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

  private TestData testData := TestData()
  private Haven haven := Haven()
}
