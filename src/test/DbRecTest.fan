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
    Grid alpha := JsonReader(File(`test_data/alpha.json`).in).readVal

    //-----------------------------------
    DbRec rec := DbRec(alpha.get(0))
    verifyEq(rec.id, "a-0000")
    verifyEq(rec.paths,
      ["id", "area", "dis", "geoAddr", "geoCity", "geoCoord", "geoCountry",
      "geoElevation", "geoPostalCode", "geoState", "geoStreet", "site", "tz",
      "weatherStationRef"])
    verifyEq(rec.pathRefs,
      [PathRef("weatherStationRef", Ref.fromStr("a-07eb"))])
    verifyEq(JsonWriter.valToStr(rec.values), """{"area":151455.0, "geoState":"CO", "geoPostalCode":"80821", "tz":"Denver", "geoCity":"Hugo", "dis":"Alpha", "geoAddr":"123 Prarie St, Hugo, CO 80821", "geoElevation":2956.0, "geoCoord":{"_kind":"coord", "lng":-103.57159, "lat":39.04532}, "geoStreet":"123 Prarie St", "geoCountry":"US"}""")
    verifyEq(JsonWriter.valToStr(rec.units), """{"area":"ft\\u00b2", "geoElevation":"m"}""")
    verifyNull(rec.spec)

    //-----------------------------------
    rec = DbRec(alpha.get(2))
    verifyEq(rec.id, "a-0002")
    verifyEq(rec.paths,
      ["id", "chilled", "cmd", "cool", "cur", "dis", "equipRef", "his", "kind",
      "point", "siteRef", "tz", "unit", "valve", "water", "custom",
      "custom.description"])
    verifyEq(rec.pathRefs,
      [PathRef("equipRef", Ref.fromStr("a-0001")),
       PathRef("siteRef", Ref.fromStr("a-0000"))])
    verifyEq(JsonWriter.valToStr(rec.values),
      """{"unit":"%", "kind":"Number", "tz":"Denver", "custom":{"description":"Clg_Valve_Cmd"}, "dis":"Alpha Airside AHU-2 Chilled Water Valve"}""")
    verifyEq(JsonWriter.valToStr(rec.units), "{}")
    verifyNull(rec.spec)
  }

  Void testNiagara()
  {
    niagara := Str:Dict[:]
    f := File(`test_data/jason.txt`)
    f.eachLine |line|
    {
      Dict rec := JsonReader(line.in).readVal
      Str id := ((Ref)rec->id).id
      niagara.add(id, rec)
    }

    DbRec rec := DbRec(niagara.get("h:2c6"))
    //echo(rec.id)
    //echo(rec.paths)
    //echo(rec.pathRefs)
    //echo(JsonWriter.valToStr(rec.values))
    //echo(JsonWriter.valToStr(rec.units))
    //echo(rec.spec)

    verifyEq(rec.id, "h:2c6")
    verifyEq(rec.paths,
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
      "slotPath"])
    verifyEq(rec.pathRefs,
      [PathRef("spec", Ref.fromStr("cc.niagara.control::NumericWritable")),
       PathRef("links.in10.fromRef", Ref.fromStr("h:2c4")),
       PathRef("meta.wsAnnotation.slotSpec", Ref.fromStr("cc.niagara.baja::WsAnnotation")),
       PathRef("parentRef", Ref.fromStr("h:2bf"))])
    verifyEq(JsonWriter.valToStr(rec.values),
"""{"compName":"damper", "overrideExpiration":{"_kind":"dateTime", "val":"1969-12-31T19:00:00-05:00", "tz":"New_York"}, "facets":{"min":{"_kind":"number", "val":"-INF"}, "max":{"_kind":"number", "val":"INF"}, "precision":1.0, "units":"null_"}, "dis":"damper", "out":{"value":0.0, "status":"ok"}, "links":{"in10":{"fromOrd":"h:2c4", "fromSlot":"out", "enabled":true}}, "wsAnnotation":"64,10,8", "kind":"Number", "in2":{"value":0.0, "status":"ok"}, "in1":{"value":0.0, "status":"ok"}, "in4":{"value":0.0, "status":"ok"}, "in3":{"value":0.0, "status":"ok"}, "in6":{"value":0.0, "status":"ok"}, "in5":{"value":0.0, "status":"ok"}, "in8":{"value":0.0, "status":"ok"}, "in7":{"value":0.0, "status":"ok"}, "in11":{"value":0.0, "status":"ok"}, "in9":{"value":0.0, "status":"ok"}, "in10":{"value":0.0, "status":"ok"}, "in13":{"value":0.0, "status":"ok"}, "meta":{"in10":{"flags":{}}, "wsAnnotation":{}}, "in12":{"value":0.0, "status":"ok"}, "in15":{"value":0.0, "status":"ok"}, "in14":{"value":0.0, "status":"ok"}, "slotPath":"slot:/AHUSystem/vavs/vav8/damper", "in16":{"value":0.0, "status":"ok"}, "fallback":{"value":76.0, "status":"ok"}}""")
    verifyEq(JsonWriter.valToStr(rec.units), "{}")
    verifyEq(rec.spec, Ref.fromStr("cc.niagara.control::NumericWritable"))
  }

  Void testNestedUnits()
  {
    json := """ { "id": { "_kind": "ref", "val": "xyz" }, "a":{"_kind":"number", "val":1, "unit":"ft\u00b2"}, "b":{"_kind":"number", "val":2}, "c": { "d":{"_kind":"number", "val":3, "unit":"m"}, "e":{"_kind":"number", "val":4} }, "f": { "g":{"_kind":"number", "val":5} } } """
    DbRec rec := DbRec(JsonReader(json.in).readVal)

    verifyEq(rec.id, "xyz")
    verifyEq(rec.paths, ["a", "b", "c", "c.d", "c.e", "f", "f.g", "id"])
    verifyEq(rec.pathRefs, PathRef[,])
    verifyEq(JsonWriter.valToStr(rec.values),
      """{"a":1.0, "b":2.0, "c":{"d":3.0, "e":4.0}, "f":{"g":5.0}}""")
    verifyEq(JsonWriter.valToStr(rec.units), """{"a":"ft\\u00b2", "c":{"d":"m"}}""")
    verifyNull(rec.spec)
  }
}
