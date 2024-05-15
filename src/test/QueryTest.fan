////
//// Copyright (c) 2024, XetoBase
//// All Rights Reserved
////
//// History:
////   6 May 2024  Mike Jarmy  Creation
////
//
//using haystack
//
//**
//** QueryTest
//**
//class QueryTest : Test
//{
//  override Void setup()
//  {
//    storeHouse.open(
//      "jdbc:postgresql://localhost/postgres",
//      "xbd",
//      "s3crkEt")
//  }
//
//  override Void teardown()
//  {
//    storeHouse.close()
//  }
//
//  Void testQuery()
//  {
//    echo("==============================================================")
//
//    doTest(
//      Filter("ahu"),
//      Query(
//        "select rec.* from rec
//         where
//           (rec.paths @> @x0::text[]);",
//        Str:Obj["x0": "{\"ahu\"}"]))
//
//    doTest(
//      Filter("facets->min"),
//      Query(
//        "select rec.* from rec
//         where
//           (rec.paths @> @x0::text[]);",
//        Str:Obj["x0": "{\"facets.min\"}"]))
//
//    doTest(
//      Filter("chilledWaterRef->chilled"),
//      Query(
//        "select rec.* from rec
//           inner join pathref p1 on p1.rec_id = rec.id
//           inner join rec     r1 on r1.id     = p1.ref_
//         where
//           ((p1.path_ = @x0) and (r1.paths @> @x1::text[]));",
//        Str:Obj[
//          "x0":"chilledWaterRef",
//          "x1":"{\"chilled\"}"]))
//
//    doTest(
//      Filter("links->in4->fromRef->meta->inA->flags->linkTarget"),
//      Query(
//        "select rec.* from rec
//           inner join pathref p1 on p1.rec_id = rec.id
//           inner join rec     r1 on r1.id     = p1.ref_
//         where
//           ((p1.path_ = @x0) and (r1.paths @> @x1::text[]));",
//        Str:Obj[
//          "x0":"links.in4.fromRef",
//          "x1":"{\"meta.inA.flags.linkTarget\"}"]))
//
//    doTest(
//      Filter("ahu and elec"),
//      Query(
//        "select rec.* from rec
//         where
//           (
//             (rec.paths @> @x0::text[])
//             and
//             (rec.paths @> @x1::text[])
//           );",
//        Str:Obj[
//          "x0":"{\"ahu\"}",
//          "x1":"{\"elec\"}"]))
//
//    doTest(
//      Filter("chilled and pump and sensor and equipRef->siteRef->site"),
//      Query(
//        "select rec.* from rec
//           inner join pathref p1 on p1.rec_id = rec.id
//           inner join rec     r1 on r1.id     = p1.ref_
//           inner join pathref p2 on p2.rec_id = r1.id
//           inner join rec     r2 on r2.id     = p2.ref_
//         where
//           (
//             (
//               (
//                 (rec.paths @> @x0::text[])
//                 and
//                 ((p1.path_ = @x1) and (p2.path_ = @x2) and (r2.paths @> @x3::text[]))
//               )
//               and
//               (rec.paths @> @x4::text[])
//             )
//             and
//             (rec.paths @> @x5::text[])
//           );",
//        Str:Obj[
//          "x0":"{\"chilled\"}",
//          "x1":"equipRef",
//          "x2":"siteRef",
//          "x3":"{\"site\"}",
//          "x4":"{\"pump\"}",
//          "x5":"{\"sensor\"}"]))
//
//    doTest(
//      Filter("links->in4->fromRef->meta->inA->flags->linkTarget and parentRef->parentRef->slotPath"),
//      Query(
//        "select rec.* from rec
//           inner join pathref p1 on p1.rec_id = rec.id
//           inner join rec     r1 on r1.id     = p1.ref_
//           inner join pathref p2 on p2.rec_id = r1.id
//           inner join rec     r2 on r2.id     = p2.ref_
//           inner join pathref p3 on p3.rec_id = r2.id
//           inner join rec     r3 on r3.id     = p3.ref_
//         where
//           (
//             ((p1.path_ = @x0) and (r1.paths @> @x1::text[]))
//             and
//             ((p2.path_ = @x2) and (p3.path_ = @x3) and (r3.paths @> @x4::text[]))
//           );",
//        Str:Obj[
//          "x0":"links.in4.fromRef",
//          "x1":"{\"meta.inA.flags.linkTarget\"}",
//          "x2":"parentRef",
//          "x3":"parentRef",
//          "x4":"{\"slotPath\"}"]))
//
//    doTest(
//      Filter("custom->description == \"Clg_Valve_Cmd\""),
//      Query(
//        "select rec.* from rec
//         where
//           (rec.values_ @> @x0::jsonb);",
//        Str:Obj[
//          "x0":"{\"custom\":{\"description\":\"Clg_Valve_Cmd\"}}"]))
//
//    doTest(
//      Filter("dis == \"Alpha Airside AHU-4\""),
//      Query(
//        "select rec.* from rec
//         where
//           (rec.values_ @> @x0::jsonb);",
//        Str:Obj[
//          "x0":"{\"dis\":\"Alpha Airside AHU-4\"}"]))
//
//    doTest(
//      Filter("parentRef->parentRef->slotPath == \"slot:/AHUSystem/vavs\""),
//      Query(
//        "select rec.* from rec
//           inner join pathref p1 on p1.rec_id = rec.id
//           inner join rec     r1 on r1.id     = p1.ref_
//           inner join pathref p2 on p2.rec_id = r1.id
//           inner join rec     r2 on r2.id     = p2.ref_
//         where
//           ((p1.path_ = @x0) and (p2.path_ = @x1) and (r2.values_ @> @x2::jsonb));",
//        Str:Obj[
//          "x0":"parentRef",
//          "x1":"parentRef",
//          "x2":"{\"slotPath\":\"slot:/AHUSystem/vavs\"}"]))
//
//    doTest(
//      Filter("area == 151455"),
//      Query(
//        "select rec.* from rec
//         where
//           (rec.values_ @> @x0::jsonb);",
//        Str:Obj[
//          "x0":"{\"area\":151455}"]))
//
//    doTest(
//      Filter("facets->precision == 1"),
//      Query(
//        "select rec.* from rec
//         where
//           (rec.values_ @> @x0::jsonb);",
//        Str:Obj[
//          "x0":"{\"facets\":{\"precision\":1}}"]))
//
//    //echo("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
//    //filter := Filter("facets->precision == 1")
//    //echo(testData.filter(filter).keys)
//    //query := Query(filter)
//    //echo(query)
//    ////echo(rawSql(query))
//    ////echo(storeHouse.select(query))
//
//    echo("==============================================================")
//  }
//
//  Void doTest(
//    Filter filter,
//    Query expQuery)
//  {
//    echo("--------------------------------------------------------------")
//    expDicts := testData.filter(filter)
//    //echo(expDicts.keys)
//
//    query := Query.fromFilter(filter)
//    echo(query)
//
//    //echo(rawSql(query))
//    exp := storeHouse.explain(rawSql(query))
//    //exp.each |s| { echo(s) }
//    verifyFalse(isSeqScan(exp))
//
//    verifyEq(query, expQuery)
//
//    found := storeHouse.select(query)
//    //echo(found)
//    verifyFound(found, expDicts)
//  }
//
//  private Void verifyFound(DbRec[] found, Ref:Dict expDicts)
//  {
//    verifyEq(found.size, expDicts.size)
//    found.each |fndRec|
//    {
//      expDict := expDicts.get(fndRec.id)
//      expRec := DbRec.fromDict(expDict)
//      verifyEq(fndRec, expRec)
//
//      // TODO transform fndRec into a Dict and verify against expDict
//    }
//  }
//
//  // This isn't actually reliable, its just a quick and dirty approach for
//  // debugging purposes
//  private Str rawSql(Query query)
//  {
//    Str s := query.sql
//    query.params.each |v,k|
//    {
//      s = s.replace("@" + k, "'$v'")
//    }
//    return s
//  }
//
//  Void testDottedPaths()
//  {
//    verifyEq(
//      QueryBuilder.dottedPaths(Filter("ahu").argA),
//      ["ahu"])
//
//    verifyEq(
//      QueryBuilder.dottedPaths(Filter("facets->min").argA),
//      ["facets.min"])
//
//    verifyEq(
//      QueryBuilder.dottedPaths(Filter("chilledWaterRef->chilled").argA),
//      ["chilledWaterRef", "chilled"])
//
//    verifyEq(
//      QueryBuilder.dottedPaths(Filter("fooOf->barRef").argA),
//      ["fooOf", "barRef"])
//
//    verifyEq(
//      QueryBuilder.dottedPaths(Filter("links->in4->fromRef->meta->inA->flags->linkTarget").argA),
//      ["links.in4.fromRef", "meta.inA.flags.linkTarget"])
//
//    verifyEq(
//      QueryBuilder.dottedPaths(Filter("equipRef->siteRef->area").argA),
//      ["equipRef", "siteRef", "area"])
//  }
//
//  // TODO why doesn't explain work?  it blows up on the second query
//  // TODO hook this to doTest()
//  Void testExplain()
//  {
//    exp := storeHouse.explain("select * from rec")
//    //exp.each |s| { echo(s) }
//    verifyTrue(isSeqScan(exp))
//
//    exp = storeHouse.explain(
//      "select * from rec
//         inner join pathref p1 on p1.rec_id = rec.id
//         inner join rec     r1 on r1.id     = p1.ref_
//       where
//         (p1.path_ = 'chilledWaterRef') and
//         (r1.paths @> '{\"chilled\"}'::text[])")
//    //exp.each |s| { echo(s) }
//    verifyFalse(isSeqScan(exp))
//  }
//
//  static Bool isSeqScan(Str[] explain)
//  {
//    res := false
//    explain.each |s| {
//      if (s.contains("Seq Scan"))
//        res = true;
//    }
//    return res;
//  }
//
//  private TestData testData := TestData()
//  private Storehouse storeHouse := Storehouse()
//}
