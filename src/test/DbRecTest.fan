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
    echo(JsonWriter.valToStr(rec.values))

    verifyEq(rec.id, "a-0002")

    verifyEq(rec.paths,
      ["id", "chilled", "cmd", "cool", "cur", "dis", "equipRef", "his", "kind",
      "point", "siteRef", "tz", "unit", "valve", "water", "custom",
      "custom.description"])

//    verifyEq(JsonWriter.valToStr(rec.values),
//      "{\"kind\":\"Number\", \"tz\":\"Denver\", \"siteRef\":{\"_kind\":\"ref\", \"val\":\"a-0000\"}, \"custom\":{\"description\":\"Clg_Valve_Cmd\"}, \"dis\":\"Alpha Airside AHU-2 Chilled Water Valve\", \"unit\":\"%\", \"equipRef\":{\"_kind\":\"ref\", \"val\":\"a-0001\"}, \"id\":{\"_kind\":\"ref\", \"val\":\"a-0002\"}}");
  }

}
