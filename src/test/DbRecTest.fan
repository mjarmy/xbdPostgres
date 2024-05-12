//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

class DbRecTest : Test
{
  Void testAlpha()
  {
    rec := DbRec.fromDict(testData.recs.get(Ref.fromStr("a-0000")))
    verifyEq(rec, DbRec(
      Ref.fromStr("a-0000"),
      ["id", "area", "dis", "geoAddr", "geoCity", "geoCoord", "geoCountry",
      "geoElevation", "geoPostalCode", "geoState", "geoStreet", "site", "tz",
      "weatherStationRef"],
JsonReader("""{"area":151455, "geoState":"CO", "geoPostalCode":"80821", "tz":"Denver", "geoCity":"Hugo", "dis":"Alpha", "geoAddr":"123 Prarie St, Hugo, CO 80821", "geoElevation":2956, "geoCoord":{"_kind":"coord", "lng":-103.57159, "lat":39.04532}, "geoStreet":"123 Prarie St", "geoCountry":"US"}""".in).readVal,
JsonReader("""{"weatherStationRef":{"_kind":"ref", "val":"a-07eb"}}""".in).readVal,
JsonReader("""{"area":"ft\\u00b2", "geoElevation":"m"}""".in).readVal,
      null))
    verifyEq(rec.pathRefs, Str:Ref[
      "weatherStationRef": Ref.fromStr("a-07eb"),
    ])

    rec = DbRec.fromDict(testData.recs.get(Ref.fromStr("a-0002")))
    verifyEq(rec, DbRec(
      Ref.fromStr("a-0002"),
      ["id", "chilled", "cmd", "cool", "cur", "dis", "equipRef", "his", "kind",
      "point", "siteRef", "tz", "unit", "valve", "water", "custom",
      "custom.description"],
JsonReader("""{"unit":"%", "kind":"Number", "tz":"Denver", "custom":{"description":"Clg_Valve_Cmd"}, "dis":"Alpha Airside AHU-2 Chilled Water Valve"}""".in).readVal,
JsonReader("""{"siteRef":{"_kind":"ref", "val":"a-0000"}, "equipRef":{"_kind":"ref", "val":"a-0001"}}""".in).readVal,
JsonReader("{}".in).readVal,
      null))

    verifyEq(rec.pathRefs, Str:Ref[
      "siteRef": Ref.fromStr("a-0000"),
      "equipRef": Ref.fromStr("a-0001"),
    ])
  }

  Void testNiagara()
  {
    DbRec rec := DbRec.fromDict(testData.recs.get(Ref.fromStr("h:2c6")))
    verifyEq(rec, DbRec(
      Ref.fromStr("h:2c6"),
      ["compName", "spec", "facets", "facets.min", "facets.max",
      "facets.precision", "facets.units", "dis", "links", "links.in10",
      "links.in10.fromOrd", "links.in10.fromSlot", "links.in10.fromRef",
      "links.in10.enabled", "id", "kind", "in2", "in2.value", "in2.status",
      "in1", "in1.value", "in1.status", "in4", "in4.value", "in4.status",
      "in3", "in3.value", "in3.status", "in6", "in6.value", "in6.status",
      "in5", "in5.value", "in5.status", "in8", "in8.value", "in8.status",
      "in7", "in7.value", "in7.status", "in11", "in11.value", "in11.status",
      "in9", "in9.value", "in9.status", "in10", "in10.value", "in10.status",
      "in13", "in13.value", "in13.status", "meta", "meta.in10",
      "meta.in10.flags", "meta.in10.flags.summary",
      "meta.in10.flags.transient", "meta.in10.flags.linkTarget",
      "meta.wsAnnotation", "meta.wsAnnotation.slotSpec", "in12", "in12.value",
      "in12.status", "in15", "in15.value", "in15.status", "in14",
      "in14.value", "in14.status", "in16", "in16.value", "in16.status",
      "fallback", "fallback.value", "fallback.status", "overrideExpiration",
      "point", "out", "out.value", "out.status", "wsAnnotation", "parentRef",
      "slotPath"],
JsonReader("""{"compName":"damper", "overrideExpiration":{"_kind":"dateTime", "val":"1969-12-31T19:00:00-05:00", "tz":"New_York"}, "facets":{"min":{"_kind":"number", "val":"-INF"}, "max":{"_kind":"number", "val":"INF"}, "precision":1.0, "units":"null_"}, "dis":"damper", "out":{"value":0.0, "status":"ok"}, "links":{"in10":{"fromOrd":"h:2c4", "fromSlot":"out", "enabled":true}}, "wsAnnotation":"64,10,8", "kind":"Number", "in2":{"value":0.0, "status":"ok"}, "in1":{"value":0.0, "status":"ok"}, "in4":{"value":0.0, "status":"ok"}, "in3":{"value":0.0, "status":"ok"}, "in6":{"value":0.0, "status":"ok"}, "in5":{"value":0.0, "status":"ok"}, "in8":{"value":0.0, "status":"ok"}, "in7":{"value":0.0, "status":"ok"}, "in11":{"value":0.0, "status":"ok"}, "in9":{"value":0.0, "status":"ok"}, "in10":{"value":0.0, "status":"ok"}, "in13":{"value":0.0, "status":"ok"}, "in12":{"value":0.0, "status":"ok"}, "in15":{"value":0.0, "status":"ok"}, "in14":{"value":0.0, "status":"ok"}, "slotPath":"slot:/AHUSystem/vavs/vav8/damper", "in16":{"value":0.0, "status":"ok"}, "fallback":{"value":76.0, "status":"ok"}}""".in).readVal,
JsonReader("""{"meta":{"wsAnnotation":{"slotSpec":{"_kind":"ref", "val":"cc.niagara.baja::WsAnnotation"}}}, "links":{"in10":{"fromRef":{"_kind":"ref", "val":"h:2c4"}}}, "spec":{"_kind":"ref", "val":"cc.niagara.control::NumericWritable"}, "parentRef":{"_kind":"ref", "val":"h:2bf"}}""".in).readVal,
    JsonReader("{}".in).readVal,
    Ref.fromStr("cc.niagara.control::NumericWritable")))

    verifyEq(rec.pathRefs, Str:Ref[
      "spec": Ref.fromStr("cc.niagara.control::NumericWritable"),
      "links.in10.fromRef": Ref.fromStr("h:2c4"),
      "meta.wsAnnotation.slotSpec": Ref.fromStr("cc.niagara.baja::WsAnnotation"),
      "parentRef": Ref.fromStr("h:2bf"),
    ])
  }

  Void testNestedUnits()
  {
    json := """ { "id": { "_kind": "ref", "val": "xyz" }, "a":{"_kind":"number", "val":1, "unit":"ft\u00b2"}, "b":{"_kind":"number", "val":2}, "c": { "d":{"_kind":"number", "val":3, "unit":"m"}, "e":{"_kind":"number", "val":4} }, "f": { "g":{"_kind":"number", "val":5} } } """

    DbRec rec := DbRec.fromDict(JsonReader(json.in).readVal)

    verifyEq(rec, DbRec(
      Ref.fromStr("xyz"),
      ["a", "b", "c", "c.d", "c.e", "f", "f.g", "id"],
      JsonReader("""{"a":1.0, "b":2.0, "c":{"d":3.0, "e":4.0}, "f":{"g":5.0}}""".in).readVal,
      JsonReader("{}".in).readVal,
      JsonReader("""{"a":"ft\\u00b2", "c":{"d":"m"}}""".in).readVal,
      null))
    verifyEq(rec.pathRefs, Str:Ref[:])
  }

  private TestData testData := TestData()
}

