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
    DbRec rec := DbRec(alpha.get(2))

    echo(rec.paths)
    echo(rec.pathRefs)
    echo(JsonWriter.valToStr(rec.values))

    verifyEq(rec.id, "a-0002")
    verifyEq(rec.paths,
      ["id", "chilled", "cmd", "cool", "cur", "dis", "equipRef", "his", "kind",
      "point", "siteRef", "tz", "unit", "valve", "water", "custom",
      "custom.description"])
    verifyEq(rec.pathRefs,
      [PathRef("equipRef", Ref.fromStr("a-0001")),
       PathRef("siteRef", Ref.fromStr("a-0000"))])
    verifyEq(JsonWriter.valToStr(rec.values),
      "{\"unit\":\"%\", \"kind\":\"Number\", \"tz\":\"Denver\", \"custom\":{\"description\":\"Clg_Valve_Cmd\"}, \"dis\":\"Alpha Airside AHU-2 Chilled Water Valve\"}")
  }

}
