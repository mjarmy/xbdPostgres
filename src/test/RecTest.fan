//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

**
** RecTest
**
class RecTest : Test
{
  Void testAlpha()
  {
    rec := Rec.fromDict(testData.recs.get(Ref.fromStr("a-0000")))
    expect := Rec(
      Ref.fromStr("a-0000"),
      Buf(), // placeholder, not needed for this test
      ["id", "area", "dis", "geoAddr", "geoCity", "geoCoord", "geoCountry",
       "geoElevation", "geoPostalCode", "geoState", "geoStreet", "site", "tz",
       "weatherStationRef"],
      JsonReader(
      """{
           "id": {
             "_kind": "ref",
             "val": "a-0000"
           },
           "area": 151455,
           "geoState": "CO",
           "geoPostalCode": "80821",
           "tz": "Denver",
           "geoCity": "Hugo",
           "dis": "Alpha",
           "geoAddr": "123 Prarie St, Hugo, CO 80821",
           "geoElevation": 2956,
           "geoCoord": {
             "_kind": "coord",
             "lng": -103.57159,
             "lat": 39.04532
           },
           "geoStreet": "123 Prarie St",
           "geoCountry": "US",
           "weatherStationRef": {
             "_kind": "ref",
             "val": "a-07eb"
           }
         }""".in).readVal,
      Str:Ref[
        "id": Ref.fromStr("a-0000"),
        "weatherStationRef": Ref.fromStr("a-07eb")
        ],
      null)
    verifyEq(expect, rec)

    rec = Rec.fromDict(testData.recs.get(Ref.fromStr("a-0002")))
    expect = Rec(
      Ref.fromStr("a-0002"),
      Buf(), // placeholder, not needed for this test
      ["id", "chilled", "cmd", "cool", "cur", "dis", "equipRef", "his", "kind",
        "point", "siteRef", "tz", "unit", "valve", "water", "custom",
        "custom.description"],
      JsonReader(
      """{
           "id": {
             "_kind": "ref",
             "val": "a-0002"
           },
           "unit": "%",
           "kind": "Number",
           "tz": "Denver",
           "custom": {
             "description": "Clg_Valve_Cmd"
           },
           "dis": "Alpha Airside AHU-2 Chilled Water Valve",
           "siteRef": {
             "_kind": "ref",
             "val": "a-0000"
           },
           "equipRef": {
             "_kind": "ref",
             "val": "a-0001"
           }
         }""".in).readVal,
      Str:Ref[
        "id": Ref.fromStr("a-0002"),
        "siteRef": Ref.fromStr("a-0000"),
        "equipRef": Ref.fromStr("a-0001"),
      ],
      null)
    verifyEq(expect, rec)
  }

  Void testNiagara()
  {
    Rec rec := Rec.fromDict(testData.recs.get(Ref.fromStr("h:2c6")))
    expect := Rec(
      Ref.fromStr("h:2c6"),
      Buf(), // placeholder, not needed for this test
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
      JsonReader(
      """{
           "compName": "damper",
           "overrideExpiration": {
             "_kind": "dateTime",
             "val": "1969-12-31T19:00:00-05:00",
             "tz": "New_York"
           },
           "spec": {
             "_kind": "ref",
             "val": "cc.niagara.control::NumericWritable"
           },
           "facets": {
             "min": {
               "_kind": "number",
               "val": "-INF"
             },
             "max": {
               "_kind": "number",
               "val": "INF"
             },
             "precision": 1,
             "units": "null_"
           },
           "dis": "damper",
           "out": {
             "value": 0,
             "status": "ok"
           },
           "links": {
             "in10": {
               "fromOrd": "h:2c4",
               "fromSlot": "out",
               "fromRef": {
                 "_kind": "ref",
                 "val": "h:2c4"
               },
               "enabled": true
             }
           },
           "wsAnnotation": "64,10,8",
           "id": {
             "_kind": "ref",
             "val": "h:2c6"
           },
           "kind": "Number",
           "in2": {
             "value": 0,
             "status": "ok"
           },
           "in1": {
             "value": 0,
             "status": "ok"
           },
           "in4": {
             "value": 0,
             "status": "ok"
           },
           "in3": {
             "value": 0,
             "status": "ok"
           },
           "parentRef": {
             "_kind": "ref",
             "val": "h:2bf"
           },
           "in6": {
             "value": 0,
             "status": "ok"
           },
           "in5": {
             "value": 0,
             "status": "ok"
           },
           "in8": {
             "value": 0,
             "status": "ok"
           },
           "in7": {
             "value": 0,
             "status": "ok"
           },
           "in11": {
             "value": 0,
             "status": "ok"
           },
           "in9": {
             "value": 0,
             "status": "ok"
           },
           "in10": {
             "value": 0,
             "status": "ok"
           },
           "in13": {
             "value": 0,
             "status": "ok"
           },
           "meta": {
             "wsAnnotation": {
               "slotSpec": {
                 "_kind": "ref",
                 "val": "cc.niagara.baja::WsAnnotation"
               }
             }
           },
           "in12": {
             "value": 0,
             "status": "ok"
           },
           "in15": {
             "value": 0,
             "status": "ok"
           },
           "in14": {
             "value": 0,
             "status": "ok"
           },
           "slotPath": "slot:/AHUSystem/vavs/vav8/damper",
           "in16": {
             "value": 0,
             "status": "ok"
           },
           "fallback": {
             "value": 76,
             "status": "ok"
           }
         }""".in).readVal,
    Str:Ref[
      "id": Ref.fromStr("h:2c6"),
      "spec": Ref.fromStr("cc.niagara.control::NumericWritable"),
      "links.in10.fromRef": Ref.fromStr("h:2c4"),
      "meta.wsAnnotation.slotSpec": Ref.fromStr("cc.niagara.baja::WsAnnotation"),
      "parentRef": Ref.fromStr("h:2bf"),
    ],
    Ref.fromStr("cc.niagara.control::NumericWritable"))
    verifyEq(expect, rec)
  }

  Void testNestedUnits()
  {
    json :=
      """{
           "id": {
             "_kind": "ref",
             "val": "xyz"
           },
           "a": {
             "_kind": "number",
             "val": 1,
             "unit": "ft²"
           },
           "b": {
             "_kind": "number",
             "val": 2
           },
           "c": {
             "d": {
               "_kind": "number",
               "val": 3,
               "unit": "m"
             },
             "e": {
               "_kind": "number",
               "val": 4
             }
           },
           "f": {
             "g": {
               "_kind": "number",
               "val": 5
             }
           }
         }"""

    Rec rec := Rec.fromDict(JsonReader(json.in).readVal)
    expect := Rec(
      Ref.fromStr("xyz"),
      Buf(), // placeholder, not needed for this test
      ["a", "b", "c", "c.d", "c.e", "f", "f.g", "id"],
      JsonReader(
        """{
             "id": {
               "_kind": "ref",
               "val": "xyz"
             },
             "a": 1,
             "b": 2,
             "c": {
               "d": 3,
               "e": 4
             },
             "f": {
               "g": 5
             }
           }""".in).readVal,
      Str:Ref[
        "id": Ref.fromStr("xyz")
      ],
      null)
    verifyEq(expect, rec)
  }

  private TestData testData := TestData()
}

