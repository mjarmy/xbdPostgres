//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack
using sql

**
** HavenTest
**
class HavenTest : Test
{
  override Void setup()
  {
    pool = HavenPool
    {
      it.uri      = "jdbc:postgresql://localhost/postgres"
      it.username = "xbd"
      it.password = "s3crkEt"
    }
    haven = Haven(pool)
  }

  override Void teardown()
  {
    pool.close()
  }

  Void testQueryEach()
  {
    count := 0
    pool.execute(|SqlConn conn|
    {
      stmt := conn.sql("select name from ref_tag")
      stmt.queryEach([:]) |r|
      {
        count++
      }
      stmt.close
    })
    verifyTrue(count > 0)

    id := null
    pool.execute(|SqlConn conn|
    {
      stmt := conn.sql("select name from ref_tag")
      id = stmt.queryEachWhile([:]) |r->Obj?|
      {
        return (r->name == "id") ? r->name : null
      }
      stmt.close
    })
    verifyEq(id, "id")
  }

  Void testRefPaths()
  {
    verifyEq(
      haven.refPaths(Filter("ahu").argA),
      ["ahu"])

    verifyEq(
      haven.refPaths(Filter("facets->min").argA),
      ["facets.min"])

    verifyEq(
      haven.refPaths(Filter("chilledWaterRef->chilled").argA),
      ["chilledWaterRef", "chilled"])

    verifyEq(
      haven.refPaths(Filter("links->in4->fromRef->meta->inA->flags->linkTarget").argA),
      ["links.in4.fromRef", "meta.inA.flags.linkTarget"])

    verifyEq(
      haven.refPaths(Filter("equipRef->siteRef->area").argA),
      ["equipRef", "siteRef", "area"])
  }

  Void testReadById()
  {
    verifyTrue(
      Etc.dictEq(
        haven.readById(ref("z0")),
        testData.recs[ref("z0")]
      ))

    verifyTrue(haven.readById(ref("bogus"), false) == null)
    verifyErr(UnknownRecErr#) { haven.readById(ref("bogus")) }
  }

  Void testReadByIds()
  {
    verifyTrue(haven.readByIds(Ref[,]).isEmpty)

    r := haven.readByIds(Ref[
      ref("bogus"),
      ref("z0")], false)
    verifyTrue(r.size == 2)
    verifyTrue(r[0] == null)
    verifyTrue(Etc.dictEq(r[1], testData.recs[ref("z0")]))

    verifyErr(UnknownRecErr#) { haven.readByIds(Ref[ref("bogus"), ref("z0")]) }

    r = haven.readByIds(Ref[
      ref("z3"),
      ref("z2"),
      ref("z1"),
      ref("z0")])
    verifyTrue(r.size == 4)
    verifyTrue(Etc.dictEq(r[0], testData.recs[ref("z3")]))
    verifyTrue(Etc.dictEq(r[1], testData.recs[ref("z2")]))
    verifyTrue(Etc.dictEq(r[2], testData.recs[ref("z1")]))
    verifyTrue(Etc.dictEq(r[3], testData.recs[ref("z0")]))

    r = haven.readByIds(Ref[
      ref("z0"),
      ref("z1"),
      ref("z2"),
      ref("z3")])
    verifyTrue(r.size == 4)
    verifyTrue(Etc.dictEq(r[0], testData.recs[ref("z0")]))
    verifyTrue(Etc.dictEq(r[1], testData.recs[ref("z1")]))
    verifyTrue(Etc.dictEq(r[2], testData.recs[ref("z2")]))
    verifyTrue(Etc.dictEq(r[3], testData.recs[ref("z3")]))
  }

  Void testRead()
  {
    verifyTrue(Etc.dictEq(
      haven.read(Filter("id == @z0")),
      testData.recs[ref("z0")]))

    verifyTrue(haven.read(Filter("id == @bogus"), false) == null)
    verifyErr(UnknownRecErr#) { haven.read(Filter("id == @bogus")) }
  }

 Void testHaven()
 {
   echo("==============================================================")

   //-----------------
   // has

   doReadAll(
     Filter("haven"),
     Query(
       "select rec.brio from rec
        where
          (rec.paths @> @x0::text[])",
       Str:Obj[
         "x0": "{\"haven\"}"
       ]))

   doReadAll(
     Filter("haven and e"),
     Query(
       "select rec.brio from rec
        where
          (
            (rec.paths @> @x0::text[])
            and
            (rec.paths @> @x1::text[])
          )",
       Str:Obj[
         "x0":"{\"e\"}",
         "x1":"{\"haven\"}"
       ]))

   doReadAll(
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
          )",
       Str:Obj[
         "x0":"{\"haven\"}",
         "x1":"{\"num\"}",
         "x2":"{\"str\"}"
       ]))

   //-----------------
   // Refs

   doReadAll(
     Filter("haven and id == @z0"),
     Query(
       "select rec.brio from rec
        where
          (
            (rec.paths @> @x0::text[])
            and
            (exists (select 1 from path_ref v1 where v1.source = rec.id and v1.path_ = @x1 and v1.target = @x2))
          )",
       Str:Obj[
         "x0":"{\"haven\"}",
         "x1":"id",
         "x2":"z0",
       ]))

   doReadAll(
     Filter("haven and id != @z0"),
     Query(
       "select rec.brio from rec
        where
          (
            (rec.paths @> @x0::text[])
            and
            (not (exists (select 1 from path_ref v1 where v1.source = rec.id and v1.path_ = @x1 and v1.target = @x2)))
          )",
       Str:Obj[
         "x0":"{\"haven\"}",
         "x1":"id",
         "x2":"z0",
       ]))

   doReadAll(
     Filter("midRef == @mid-1"),
     Query(
       "select rec.brio from rec
        where
          (exists (select 1 from path_ref v1 where v1.source = rec.id and v1.path_ = @x0 and v1.target = @x1))",
       Str:Obj[
         "x0":"midRef",
         "x1":"mid-1",
       ]))

   doReadAll(
     Filter("midRef == @mid-2"),
     Query(
       "select rec.brio from rec
        where
          (exists (select 1 from path_ref v1 where v1.source = rec.id and v1.path_ = @x0 and v1.target = @x1))",
       Str:Obj[
         "x0":"midRef",
         "x1":"mid-2",
       ]))

   doReadAll(
     Filter("midRef->dis == \"Mid 1\""),
     Query(
       "select rec.brio from rec
        where
          (exists (
            select 1 from path_ref p1
            inner join rec r1 on r1.id = p1.target
            where (p1.source = rec.id) and (p1.path_ = @x0) and (r1.strs @> @x1::jsonb)))",
       Str:Obj[
         "x0":"midRef",
         "x1":"{\"dis\":\"Mid 1\"}",
       ]))

   doReadAll(
     Filter("midRef->dis == \"Mid 2\""),
     Query(
       "select rec.brio from rec
        where
          (exists (
            select 1 from path_ref p1
            inner join rec r1 on r1.id = p1.target
            where (p1.source = rec.id) and (p1.path_ = @x0) and (r1.strs @> @x1::jsonb)))",
       Str:Obj[
         "x0":"midRef",
         "x1":"{\"dis\":\"Mid 2\"}",
       ]))

   doReadAll(
     Filter("midRef->topRef == @top-1"),
     Query(
       "select rec.brio from rec
        where
          (exists (
            select 1 from path_ref p1
            inner join rec r1 on r1.id = p1.target
            where (p1.source = rec.id) and (p1.path_ = @x0) and (exists (select 1 from path_ref v1 where v1.source = r1.id and v1.path_ = @x1 and v1.target = @x2))))",
       Str:Obj[
         "x0":"midRef",
         "x1":"topRef",
         "x2":"top-1",
       ]))

   doReadAll(
     Filter("midRef->topRef == @top-2"),
     Query(
       "select rec.brio from rec
        where
          (exists (
            select 1 from path_ref p1
            inner join rec r1 on r1.id = p1.target
            where (p1.source = rec.id) and (p1.path_ = @x0) and (exists (select 1 from path_ref v1 where v1.source = r1.id and v1.path_ = @x1 and v1.target = @x2))))",
       Str:Obj[
         "x0":"midRef",
         "x1":"topRef",
         "x2":"top-2",
       ]))

   doReadAll(
     Filter("midRef->topRef->dis == \"Top 1\""),
     Query(
       "select rec.brio from rec
        where
          (exists (
            select 1 from path_ref p1
            inner join rec r1 on r1.id = p1.target
            inner join path_ref p2 on p2.source = r1.id
            inner join rec r2 on r2.id = p2.target
            where (p1.source = rec.id) and (p1.path_ = @x0) and (p2.path_ = @x1) and (r2.strs @> @x2::jsonb)))",
       Str:Obj[
         "x0":"midRef",
         "x1":"topRef",
         "x2":"{\"dis\":\"Top 1\"}",
       ]))

   doReadAll(
     Filter("midRef->topRef->dis == \"Top 2\""),
     Query(
       "select rec.brio from rec
        where
          (exists (
            select 1 from path_ref p1
            inner join rec r1 on r1.id = p1.target
            inner join path_ref p2 on p2.source = r1.id
            inner join rec r2 on r2.id = p2.target
            where (p1.source = rec.id) and (p1.path_ = @x0) and (p2.path_ = @x1) and (r2.strs @> @x2::jsonb)))",
       Str:Obj[
         "x0":"midRef",
         "x1":"topRef",
         "x2":"{\"dis\":\"Top 2\"}",
       ]))

   //-----------------
   // Uri

   doReadAll(
     Filter("haven and b == `https://project-haystack.org/`"),
     Query(
       "select rec.brio from rec
        where
          (
            (rec.paths @> @x0::text[])
            and
            (rec.uris @> @x1::jsonb)
          )",
       Str:Obj[
         "x0":"{\"haven\"}",
         "x1":"{\"b\":\"https://project-haystack.org/\"}",
       ]))

   doReadAll(
     Filter("haven and b != `https://project-haystack.org/`"),
     Query(
       "select rec.brio from rec
        where
          (
            (rec.paths @> @x0::text[])
            and
            ((rec.paths @> @x1::text[]) and ((rec.uris is null) or (not (rec.uris @> @x2::jsonb))))
          )",
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
     doReadAll(
       Filter("haven and $arrows == \"y\""),
       Query(
         "select rec.brio from rec
          where
            (
              (rec.paths @> @x0::text[])
              and
              (rec.strs @> @x1::jsonb)
            )",
         Str:Obj[
           "x0":"{\"haven\"}",
           "x1":"{\"$dotted\":\"y\"}",
         ]))

     // !=
     doReadAll(
       Filter("haven and $arrows != \"y\""),
       Query(
         "select rec.brio from rec
          where
            (
              (rec.paths @> @x0::text[])
              and
              ((rec.paths @> @x1::text[]) and ((rec.strs is null) or (not (rec.strs @> @x2::jsonb))))
            )",
         Str:Obj[
           "x0":"{\"haven\"}",
           "x1":"{\"$dotted\"}",
           "x2":"{\"$dotted\":\"y\"}",
         ]))

     // cmp
     ["<", "<=", ">", ">="].each |op|
     {
       doReadAll(
         Filter("haven and $arrows $op \"y\""),
         Query(
           "select rec.brio from rec
            where
              (
                (rec.paths @> @x0::text[])
                and
                ((rec.paths @> @x1::text[]) and ((rec.strs ->> @x2) $op @x3))
              )",
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
     doReadAll(
       Filter("haven and num == $num"),
       Query(
         "select rec.brio from rec
          where
            (
              (rec.paths @> @x0::text[])
              and
              ((rec.nums @> @x1::jsonb) and (rec.units @> @x2::jsonb))
            )",
         Str:Obj[
           "x0":"{\"haven\"}",
           "x1":"{\"num\":${num.toFloat}}",
           "x2":"{\"num\":$uparam}",
         ]))

     // !=
     doReadAll(
       Filter("haven and num != $num"),
       Query(
         "select rec.brio from rec
          where
            (
              (rec.paths @> @x0::text[])
              and
              ((rec.paths @> @x1::text[]) and ((rec.nums is null) or (not ((rec.nums @> @x2::jsonb) and (rec.units @> @x3::jsonb)))))
            )",
         Str:Obj[
           "x0":"{\"haven\"}",
           "x1":"{\"num\"}",
           "x2":"{\"num\":${num.toFloat}}",
           "x3":"{\"num\":$uparam}",
         ]))

     // cmp
     ["<", "<=", ">", ">="].each |op|
     {
       doReadAll(
         Filter("haven and num $op $num"),
         Query(
           "select rec.brio from rec
            where
              (
                (rec.paths @> @x0::text[])
                and
                ((rec.paths @> @x1::text[]) and (((rec.nums -> @x2)::real) $op @x3) and (rec.units @> @x4::jsonb))
              )",
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
   doReadAll(
     Filter("haven and bool == true"),
     Query(
       "select rec.brio from rec
        where
          (
            (rec.paths @> @x0::text[])
            and
            (rec.bools @> @x1::jsonb)
          )",
       Str:Obj[
         "x0":"{\"haven\"}",
         "x1":"{\"bool\":true}",
       ]))

   // !=
   doReadAll(
     Filter("haven and bool != true"),
     Query(
       "select rec.brio from rec
        where
          (
            (rec.paths @> @x0::text[])
            and
            ((rec.paths @> @x1::text[]) and ((rec.bools is null) or (not (rec.bools @> @x2::jsonb))))
          )",
       Str:Obj[
         "x0":"{\"haven\"}",
         "x1":"{\"bool\"}",
         "x2":"{\"bool\":true}",
       ]))

   // cmp
   ["<", "<=", ">", ">="].each |op|
   {
     doReadAll(
       Filter("haven and bool $op true"),
       Query(
         "select rec.brio from rec
          where
            (
              (rec.paths @> @x0::text[])
              and
              ((rec.paths @> @x1::text[]) and (((rec.bools -> @x2)::boolean) $op @x3))
            )",
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
     doReadAll(
       Filter("haven and $arrows == 2021-03-22"),
       Query(
         "select rec.brio from rec
          where
            (
              (rec.paths @> @x0::text[])
              and
              (rec.dates @> @x1::jsonb)
            )",
         Str:Obj[
           "x0":"{\"haven\"}",
           "x1":"{\"$dotted\":\"2021-03-22\"}",
         ]))

     // !=
     doReadAll(
       Filter("haven and $arrows != 2021-03-22"),
       Query(
         "select rec.brio from rec
          where
            (
              (rec.paths @> @x0::text[])
              and
              ((rec.paths @> @x1::text[]) and ((rec.dates is null) or (not (rec.dates @> @x2::jsonb))))
            )",
         Str:Obj[
           "x0":"{\"haven\"}",
           "x1":"{\"$dotted\"}",
           "x2":"{\"$dotted\":\"2021-03-22\"}",
         ]))

     // cmp
     ["<", "<=", ">", ">="].each |op|
     {
       doReadAll(
         Filter("haven and $arrows $op 2021-03-22"),
         Query(
           "select rec.brio from rec
            where
              (
                (rec.paths @> @x0::text[])
                and
                ((rec.paths @> @x1::text[]) and ((rec.dates ->> @x2) $op @x3))
              )",
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
     doReadAll(
       Filter("haven and $arrows == 17:19:23"),
       Query(
         "select rec.brio from rec
          where
            (
              (rec.paths @> @x0::text[])
              and
              (rec.times @> @x1::jsonb)
            )",
         Str:Obj[
           "x0":"{\"haven\"}",
           "x1":"{\"$dotted\":\"17:19:23\"}",
         ]))

     // !=
     doReadAll(
       Filter("haven and $arrows != 17:19:23"),
       Query(
         "select rec.brio from rec
          where
            (
              (rec.paths @> @x0::text[])
              and
              ((rec.paths @> @x1::text[]) and ((rec.times is null) or (not (rec.times @> @x2::jsonb))))
            )",
         Str:Obj[
           "x0":"{\"haven\"}",
           "x1":"{\"$dotted\"}",
           "x2":"{\"$dotted\":\"17:19:23\"}",
         ]))

     // cmp
     ["<", "<=", ">", ">="].each |op|
     {
       doReadAll(
         Filter("haven and $arrows $op 17:19:23"),
         Query(
           "select rec.brio from rec
            where
              (
                (rec.paths @> @x0::text[])
                and
                ((rec.paths @> @x1::text[]) and ((rec.times ->> @x2) $op @x3))
              )",
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
   doReadAll(
     Filter.has("haven").and(Filter.eq("quux", ts)),
     Query(
       "select rec.brio from rec
        where
          (
            (rec.paths @> @x0::text[])
            and
            (rec.dateTimes @> @x1::jsonb)
          )",
       Str:Obj[
         "x0":"{\"haven\"}",
         "x1":"{\"quux\":669763163000}",
       ]))

   // !=
   doReadAll(
     Filter.has("haven").and(Filter.ne("quux", ts)),
     Query(
       "select rec.brio from rec
        where
          (
            (rec.paths @> @x0::text[])
            and
            ((rec.paths @> @x1::text[]) and ((rec.dateTimes is null) or (not (rec.dateTimes @> @x2::jsonb))))
          )",
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
     doReadAll(
       Filter.has("haven").and(f),
       Query(
         "select rec.brio from rec
          where
            (
              (rec.paths @> @x0::text[])
              and
              ((rec.paths @> @x1::text[]) and (((rec.dateTimes -> @x2)::bigint) $op @x3))
            )",
         Str:Obj[
           "x0":"{\"haven\"}",
           "x1":"{\"quux\"}",
           "x2":"quux",
           "x3":669763163000,
         ]))
   }

   echo("==============================================================")
 }

 Void testMismatchedType()
 {
   doReadAll(
     Filter("haven and str == 2"),
     Query(
       "select rec.brio from rec
        where
          (
            (rec.paths @> @x0::text[])
            and
            ((rec.nums @> @x1::jsonb) and (rec.units @> @x2::jsonb))
          )",
       Str:Obj[
         "x0":"{\"haven\"}",
         "x1":"{\"str\":2.0}",
         "x2":"{\"str\":null}",
       ]))
 }

 Void testAlpha()
 {
   echo("==============================================================")

   doReadAll(
     Filter("ahu"),
     Query(
       "select rec.brio from rec
        where
          (rec.paths @> @x0::text[])",
       Str:Obj["x0": "{\"ahu\"}"]))

   doReadAll(
     Filter("chilledWaterRef->chilled"),
     Query(
       "select rec.brio from rec
        where
          (exists (
            select 1 from path_ref p1
            inner join rec r1 on r1.id = p1.target
            where (p1.source = rec.id) and (p1.path_ = @x0) and (r1.paths @> @x1::text[])))",
       Str:Obj[
         "x0":"chilledWaterRef",
         "x1":"{\"chilled\"}"]))

   doReadAll(
     Filter("ahu and elec"),
     Query(
       "select rec.brio from rec
        where
          (
            (rec.paths @> @x0::text[])
            and
            (rec.paths @> @x1::text[])
          )",
       Str:Obj[
         "x0":"{\"ahu\"}",
         "x1":"{\"elec\"}"]))

   doReadAll(
     Filter("chilled and pump and sensor and equipRef->siteRef->site"),
     Query(
       "select rec.brio from rec
        where
          (
            (
              (
                (rec.paths @> @x0::text[])
                and
                (exists (
                  select 1 from path_ref p1
                  inner join rec r1 on r1.id = p1.target
                  inner join path_ref p2 on p2.source = r1.id
                  inner join rec r2 on r2.id = p2.target
                  where (p1.source = rec.id) and (p1.path_ = @x1) and (p2.path_ = @x2) and (r2.paths @> @x3::text[])))
              )
              and
              (rec.paths @> @x4::text[])
            )
            and
            (rec.paths @> @x5::text[])
          )",
       Str:Obj[
         "x0":"{\"chilled\"}",
         "x1":"equipRef",
         "x2":"siteRef",
         "x3":"{\"site\"}",
         "x4":"{\"pump\"}",
         "x5":"{\"sensor\"}"]))

   doReadAll(
     Filter("custom->description == \"Clg_Valve_Cmd\""),
     Query(
       "select rec.brio from rec
        where
          (rec.strs @> @x0::jsonb)",
       Str:Obj[
         "x0": """{"custom.description":"Clg_Valve_Cmd"}"""
       ]))

   doReadAll(
     Filter("dis == \"Alpha Airside AHU-4\""),
     Query(
       "select rec.brio from rec
        where
          (rec.strs @> @x0::jsonb)",
       Str:Obj[
         "x0":"{\"dis\":\"Alpha Airside AHU-4\"}"]))

   doReadAll(
     Filter("geoElevation == 2956m"),
     Query(
       "select rec.brio from rec
        where
          ((rec.nums @> @x0::jsonb) and (rec.units @> @x1::jsonb))",
       Str:Obj[
         "x0":"{\"geoElevation\":2956.0}",
         "x1":"{\"geoElevation\":\"m\"}"
       ]))

   doReadAll(
     Filter("equipRef == @a-0039"),
     Query(
       "select rec.brio from rec
        where
          (exists (select 1 from path_ref v1 where v1.source = rec.id and v1.path_ = @x0 and v1.target = @x1))",
       Str:Obj[
         "x0":"equipRef",
         "x1":"a-0039"
       ]))

   doReadAll(
     Filter("equipRef->dis == \"Alpha Airside AHU-4\""),
     Query(
       "select rec.brio from rec
        where
          (exists (
            select 1 from path_ref p1
            inner join rec r1 on r1.id = p1.target
            where (p1.source = rec.id) and (p1.path_ = @x0) and (r1.strs @> @x1::jsonb)))",
       Str:Obj[
         "x0":"equipRef",
         "x1":"{\"dis\":\"Alpha Airside AHU-4\"}"
       ]))

   doReadAll(
     Filter("id->area"),
     Query(
       "select rec.brio from rec
        where
          (exists (
            select 1 from path_ref p1
            inner join rec r1 on r1.id = p1.target
            where (p1.source = rec.id) and (p1.path_ = @x0) and (r1.paths @> @x1::text[])))",
       Str:Obj[
         "x0":"id",
         "x1":"{\"area\"}"]))
 }

 Void testSeqScan()
 {
   echo("==============================================================")

   doReadAll(
     Filter("not point"),
     Query(
       "select rec.brio from rec
        where
          (not (rec.paths @> @x0::text[]))",
       Str:Obj[
         "x0":"{\"point\"}"]),
       true)
 }

 Void testNiagara()
 {
   echo("==============================================================")

   doReadAll(
     Filter("facets->min"),
     Query(
       "select rec.brio from rec
        where
          (rec.paths @> @x0::text[])",
       Str:Obj["x0": "{\"facets.min\"}"]))

   doReadAll(
     Filter("links->in4->fromRef->meta->inA->flags->linkTarget"),
     Query(
       "select rec.brio from rec
        where
          (exists (
            select 1 from path_ref p1
            inner join rec r1 on r1.id = p1.target
            where (p1.source = rec.id) and (p1.path_ = @x0) and (r1.paths @> @x1::text[])))",
       Str:Obj[
         "x0":"links.in4.fromRef",
         "x1":"{\"meta.inA.flags.linkTarget\"}"]))

   doReadAll(
     Filter("links->in4->fromRef->meta->inA->flags->linkTarget and parentRef->parentRef->slotPath"),
     Query(
       "select rec.brio from rec
        where
          (
            (exists (
              select 1 from path_ref p1
              inner join rec r1 on r1.id = p1.target
              where (p1.source = rec.id) and (p1.path_ = @x0) and (r1.paths @> @x1::text[])))
            and
            (exists (
              select 1 from path_ref p2
              inner join rec r2 on r2.id = p2.target
              inner join path_ref p3 on p3.source = r2.id
              inner join rec r3 on r3.id = p3.target
              where (p2.source = rec.id) and (p2.path_ = @x2) and (p3.path_ = @x3) and (r3.paths @> @x4::text[])))
          )",
       Str:Obj[
         "x0":"links.in4.fromRef",
         "x1":"{\"meta.inA.flags.linkTarget\"}",
         "x2":"parentRef",
         "x3":"parentRef",
         "x4":"{\"slotPath\"}"]))

   doReadAll(
     Filter("parentRef->parentRef->slotPath == \"slot:/AHUSystem/vavs\""),
     Query(
       "select rec.brio from rec
        where
          (exists (
            select 1 from path_ref p1
            inner join rec r1 on r1.id = p1.target
            inner join path_ref p2 on p2.source = r1.id
            inner join rec r2 on r2.id = p2.target
            where (p1.source = rec.id) and (p1.path_ = @x0) and (p2.path_ = @x1) and (r2.strs @> @x2::jsonb)))",
       Str:Obj[
         "x0":"parentRef",
         "x1":"parentRef",
         "x2":"{\"slotPath\":\"slot:/AHUSystem/vavs\"}"]))

   doReadAll(
     Filter("facets->precision == 1"),
     Query(
       "select rec.brio from rec
        where
          ((rec.nums @> @x0::jsonb) and (rec.units @> @x1::jsonb))",
       Str:Obj[
         "x0":"{\"facets.precision\":1.0}",
         "x1":"{\"facets.precision\":null}",
       ]))
 }

 Void testDemogen()
 {
   echo("==============================================================")

   filter := Filter("elec and sensor and equipRef->siteRef->area < 10000ft²")

   doReadAll(
     filter,
     Query(
       "select rec.brio from rec
        where
          (
            (
              (rec.paths @> @x0::text[])
              and
              (rec.paths @> @x1::text[])
            )
            and
            (exists (
              select 1 from path_ref p1
              inner join rec r1 on r1.id = p1.target
              inner join path_ref p2 on p2.source = r1.id
              inner join rec r2 on r2.id = p2.target
              where (p1.source = rec.id) and (p1.path_ = @x2) and (p2.path_ = @x3) and ((r2.paths @> @x4::text[]) and (((r2.nums -> @x5)::real) < @x6) and (r2.units @> @x7::jsonb))))
          )",
       Str:Obj[
         "x0":"{\"elec\"}",
         "x1":"{\"sensor\"}",
         "x2":"equipRef",
         "x3":"siteRef",
         "x4":"{\"area\"}",
         "x5":"area",
         "x6":10000.0f,
         "x7":"{\"area\":\"ft\\u00b2\"}"
       ]))

   // readAll
   verifyEq(haven.readAll(filter).size, 10)
   verifyEq(haven.readAll(filter, Etc.dict1("limit", 5)).size, 5)
   verifyEq(haven.readAll(filter, Etc.dict1("limit", 0)).size, 0)

   // readCount
   verifyEq(haven.readCount(filter), 10)
   verifyEq(haven.readCount(filter, Etc.dict1("limit", 5)), 5)
   verifyEq(haven.readCount(filter, Etc.dict1("limit", 0)), 0)

   // readEach
   found := Dict[,]
   haven.readEach(filter, null, |d| { found.add(d) })
   verifyDictsEq(testData.filter(filter), found)

   count := 0
   haven.readEach(filter, Etc.dict1("limit", 5), |d| { count++ })
   verifyEq(count, 5)

   // readEachWhile
   found = Dict[,]
   haven.readEachWhile(filter, null,
     |d->Obj?| { found.add(d); return null })
   verifyDictsEq(testData.filter(filter), found)

   count = 0
   res := haven.readEachWhile(filter, null,
     |d->Obj?| { return count++ == 8 ? 8 : null })
   verifyEq(res, 8)

   count = 0
   res = haven.readEachWhile(filter, Etc.dict1("limit", 5),
     |d->Obj?| { return count++ == 8 ? 8 : null })
   verifyEq(res, null)
 }

  Void testSpec()
  {
    echo("==============================================================")

    doReadAll(
      Filter("ph::Sensor"),
      Query(
        "select rec.brio from rec
         where
           (exists (select 1 from spec s1 where s1.qname = rec.spec and s1.inherits_from = @x0))",
        Str:Obj[
          "x0":"ph::Sensor",
        ]),
        true)

    doReadAll(
      Filter("ph.points::AirFlowSensor"),
      Query(
        "select rec.brio from rec
         where
           (exists (select 1 from spec s1 where s1.qname = rec.spec and s1.inherits_from = @x0))",
        Str:Obj[
          "x0":"ph.points::AirFlowSensor",
        ]))

    doReadAll(
      Filter("ph.points::AirPressureSensor and equipRef == @p:demo:r:2de0dfb5-6e04b073"),
      Query(
        "select rec.brio from rec
         where
           (
             (exists (select 1 from path_ref v1 where v1.source = rec.id and v1.path_ = @x0 and v1.target = @x1))
             and
             (exists (select 1 from spec s1 where s1.qname = rec.spec and s1.inherits_from = @x2))
           )",
        Str:Obj[
          "x0":"equipRef",
          "x1":"p:demo:r:2de0dfb5-6e04b073",
          "x2":"ph.points::AirPressureSensor",
        ]))
  }

//  Void testCrud()
//  {
//    echo("==============================================================")
//
//    x0 := Ref("x0")
//
//    verifyErr(InvalidRecErr#) { haven.create(Etc.dict1("id", x0)) }
//
//    // create
//    verifyTrue(Etc.dictEq(
//      haven.create(Etc.emptyDict, x0),
//      Etc.dict1("id", x0)))
//
//    // read
//    verifyTrue(Etc.dictEq(
//      haven.readById(x0),
//      Etc.dict1("id", x0)))
//
//    verifyEq(selectPathRefs(x0), ["id":["x0"]])
//
//    // update (no refs)
//    verifyTrue(Etc.dictEq(
//      haven.update(x0, Etc.dict1("foo", "bar"), null),
//      Etc.dict2("id", x0, "foo", "bar")))
//    verifyEq(selectPathRefs(x0), ["id":["x0"]])
//
//    verifyTrue(Etc.dictEq(
//      haven.update(x0, Etc.dict1("foo", Remove.val), null),
//      Etc.dict1("id", x0)))
//    verifyEq(selectPathRefs(x0), ["id":["x0"]])
//
//    // update (ref)
//    verifyTrue(Etc.dictEq(
//      haven.update(x0, Etc.dict1("foo", ref("bar")), null),
//      Etc.dict2("id", x0, "foo", ref("bar"))))
//    verifyEq(selectPathRefs(x0), ["id":["x0"], "foo":["bar"]])
//
//    verifyTrue(Etc.dictEq(
//      haven.update(x0, Etc.dict1("foo", Remove.val), null),
//      Etc.dict1("id", x0)))
//    verifyEq(selectPathRefs(x0), ["id":["x0"]])
//
//    // update (ref list)
//    verifyTrue(Etc.dictEq(
//      haven.update(x0, Etc.dict1("foo", [ref("bar"), ref("quux")]), null),
//      Etc.dict2("id", x0, "foo", [ref("bar"), ref("quux")])))
//    verifyEq(selectPathRefs(x0), ["id":["x0"], "foo":["bar", "quux"]])
//
//    verifyTrue(Etc.dictEq(
//      haven.update(x0, Etc.dict1("foo", Remove.val), null),
//      Etc.dict1("id", x0)))
//    verifyEq(selectPathRefs(x0), ["id":["x0"]])
//
//    // delete
//    haven.delete(x0)
//    verifyNull(haven.readById(x0, false))
//  }
//
//  private Str:Str[] selectPathRefs(Ref source)
//  {
//    refs  := Str:Str[][:]
//
//    stmt := haven.testConn.sql(
//      "select path_, target from path_ref where source = @source"
//    ).prepare
//
//    stmt.query(["source": source.id]).each |r|
//    {
//      Str path := r->path_
//      Str target := r->target
//
//      if (refs.containsKey(path))
//      {
//        refs[path].add(target)
//        refs[path].sort
//      }
//      else
//      {
//        refs[path] = [target]
//      }
//    }
//
//    stmt.close
//
//    return refs
//  }

  private Void doReadAll(
    Filter filter,
    Query expectedQuery,
    Bool allowSequential := false)
  {
    echo("--------------------------------------------------------------")
    echo("Filter: '$filter'")
    verbose

    // Dump the expected query
    verbose("-------------------------------------")
    verbose("expected query")
    verbose
    dumpQuery(expectedQuery)
    verbose

    // Construct the Query
    query := Query.fromFilter(haven, filter)
    verbose("-------------------------------------")
    verbose("found query")
    verbose
    dumpQuery(query)
    verbose

    // get the raw sql
    raw := rawSql(query)
    verbose("-------------------------------------")
    verbose("explain (analyze true, verbose true, buffers true) ")
    verbose(raw)
    verbose

    // make sure the queries are equal
    verbose("-------------------------------------")
    verbose("sql eq ${expectedQuery.sql == query.sql}")
    verbose("query params eq ${expectedQuery.params == query.params}")
    verbose
    verifyEq(expectedQuery, query)

    // Explain the Query's raw sql to make sure its not a sequential scan
    verbose("-------------------------------------")
    explained := explain(raw)

    seq := isSeqScan(explained)
    if (seq) echo("************ SEQUENTIAL ************")
    if (allowSequential)
      verifyTrue(seq)
    else
      verifyFalse(seq)

    explained.each |s| {
      if (s.startsWith("Planning Time:") || s.startsWith("Execution Time:"))
        echo(s)
    }
    verbose

    // Fetch the expected data
    expected := testData.filter(filter)

    // Perfom the query in the database
    found := haven.readAll(filter)

    // Make sure the results match the test data
    verbose("-------------------------------------")
    verifyDictsEq(expected, found)
  }

  private Void verifyDictsEq(Dict[] expected, Dict[] found)
  {
    expected.sort |Dict x, Dict y->Int| { return x.id.id <=> y.id.id }
    found.sort    |Dict x, Dict y->Int| { return x.id.id <=> y.id.id }

    verbose("expected ${expected.size} rows")
    verbose(expected.map |Dict v->Ref| { v.id })
    verbose("found ${found.size} rows")
    verbose(found.map |Dict v->Ref| { v.id })

    verifyEq(expected.size, found.size)
    expected.each |dict, i|
    {
      //verbose("$i ${expected[i]}, ${found[i]} --> ${expected[i] == found[i]}")
      verifyTrue(Etc.dictEq(expected[i], found[i]))
    }
  }

  **
  ** Explain a select
  **
  Str[] explain(Str rawSql)
  {
    res := Str[,]

    pool.execute(|SqlConn conn| {
      stmt := conn.sql(
          "explain (analyze true, verbose true, buffers true) " +
          rawSql)
      stmt.query().each |row|
      {
        col := row.col("QUERY PLAN")
        res.add(row[col])
      }
      stmt.close
    })

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
    return s + ";"
  }

  private static Bool isSeqScan(Str[] explain)
  {
    res := false
    explain.each |s| {
      if (s.contains("Seq Scan"))
        res = true
    }
    return res
  }

  private static Void dumpQuery(Query query)
  {
    verbose(query.sql)
    verbose
    query.params.each | val, key |
    {
      verbose("$key: ${val.typeof} $val")
    }
  }

  private static Void verbose(Obj o := "")
  {
    //echo(o.toStr)
  }

  private static Marker M() { Marker.val }
  private static Ref ref(Str str) { Ref.fromStr(str) }
  private static Number n(Num val, Str? unit := null)
  {
    Number.makeNum(val, unit == null ? null : Unit.fromStr(unit))
  }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  private TestData testData := TestData()
  private SqlConnPool? pool
  private Haven? haven
}
