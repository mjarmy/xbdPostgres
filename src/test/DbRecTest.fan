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

    DbRec rec := DbRec(alpha.get(0))
    //echo(rec.id)
    //echo(rec.paths)
    //echo(rec.pathRefs)
    //echo(JsonWriter.valToStr(rec.values))
    //echo(JsonWriter.valToStr(rec.units))
    verifyEq(rec.id, "a-0000")
    verifyEq(rec.paths,
      ["id", "area", "dis", "geoAddr", "geoCity", "geoCoord", "geoCountry",
      "geoElevation", "geoPostalCode", "geoState", "geoStreet", "site", "tz",
      "weatherStationRef"])
    verifyEq(rec.pathRefs,
      [PathRef("weatherStationRef", Ref.fromStr("a-07eb"))])
    verifyEq(JsonWriter.valToStr(rec.values), """{"area":151455.0, "geoState":"CO", "geoPostalCode":"80821", "tz":"Denver", "geoCity":"Hugo", "dis":"Alpha", "geoAddr":"123 Prarie St, Hugo, CO 80821", "geoElevation":2956.0, "geoCoord":{"_kind":"coord", "lng":-103.57159, "lat":39.04532}, "geoStreet":"123 Prarie St", "geoCountry":"US"}""")
    verifyEq(JsonWriter.valToStr(rec.units), """{"area":"ft\\u00b2", "geoElevation":"m"}""")

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
  }

  Void testNestedUnits()
  {
    json := """ { "id": { "_kind": "ref", "val": "xyz" }, "a":{"_kind":"number", "val":1, "unit":"ft\u00b2"}, "b":{"_kind":"number", "val":2}, "c": { "d":{"_kind":"number", "val":3, "unit":"m"}, "e":{"_kind":"number", "val":4} }, "f": { "g":{"_kind":"number", "val":5} } } """
    DbRec rec := DbRec(JsonReader(json.in).readVal)
    echo(rec.id)
    echo(rec.paths)
    echo(rec.pathRefs)
    echo(JsonWriter.valToStr(rec.values))
    echo(JsonWriter.valToStr(rec.units))

    verifyEq(rec.id, "xyz")
    verifyEq(rec.paths, ["a", "b", "c", "c.d", "c.e", "f", "f.g", "id"])
    verifyEq(rec.pathRefs, PathRef[,])
    verifyEq(JsonWriter.valToStr(rec.values),
      """{"a":1.0, "b":2.0, "c":{"d":3.0, "e":4.0}, "f":{"g":5.0}}""")
    verifyEq(JsonWriter.valToStr(rec.units), """{"a":"ft\\u00b2", "c":{"d":"m"}}""")
  }

}
