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
  Void testRec()
  {
    verifyEq(
      Rec.fromDict(testData.recs[ref("z0")]),
      Rec(
        "z0",
        Str["id", "haven"],
        Str:Str[]["id":Str["z0"]],
        Str:Str[:],
        Str:Float[:],
        Str:Str?[:],
        Str:Bool[:],
        Str:Str[:],
        Str:Str[:],
        Str:Str[:],
        Str:Int[:]
      ))

    verifyEq(
      Rec.fromDict(testData.recs[ref("z1")]),
      Rec(
        "z1",
        Str["id", "haven", "a", "b", "b.c", "b.d", "b.e", "f", "g", "g.h", "g.h.i", "g.h.j"],
        Str:Str[]["id":["z1"], "b.d":["z1"]],
        Str:Str["a":"x", "b.c":"y", "g.h.j":"z"],
        Str:Float[:],
        Str:Str?[:],
        Str:Bool[:],
        Str:Str[:],
        Str:Str[:],
        Str:Str[:],
        Str:Int[:]
      )
    )

    verifyEq(
      Rec.fromDict(testData.recs[ref("z2")]),
      Rec(
        "z2",
        Str["id", "haven", "a", "b", "b.c", "b.d", "b.e"],
        Str:Str[]["id":["z2"]],
        Str:Str[:],
        Str:Float["a":1.0f, "b.c":2.0f, "b.d":3.0f, "b.e":4.0f],
        Str:Str?["a":null, "b.c":"m", "b.d":null, "b.e":"F"],
        Str:Bool[:],
        Str:Str[:],
        Str:Str[:],
        Str:Str[:],
        Str:Int[:]
      )
    )

    verifyEq(
      Rec.fromDict(testData.recs[ref("z3")]),
      Rec(
        "z3",
        Str["id", "haven", "a", "b", "c", "d", "e", "e.date", "e.time"],
        Str:Str[]["id":["z3"]],
        Str:Str[:],
        Str:Float[:],
        Str:Str?[:],
        Str:Bool["a":true],
        Str:Str["b":"https://project-haystack.org/"],
        Str:Str["c":"2021-03-22", "e.date":"2021-03-22"],
        Str:Str["d":"17:19:23","e.time":"13:57:00.381"],
        Str:Int["e":669751020381]
      )
    )

    verifyEq(
      Rec.fromDict(testData.recs[ref("top-1")]),
      Rec(
        "top-1",
        Str["id", "haven", "top", "dis", "bogus"],
        Str:Str[]["id":Str["top-1"]],
        Str:Str["dis":"Top 1"],
        Str:Float[:],
        Str:Str?[:],
        Str:Bool[:],
        Str:Str[:],
        Str:Str[:],
        Str:Str[:],
        Str:Int[:]
      ))

    verifyEq(
      Rec.fromDict(testData.recs[ref("mid-2")]),
      Rec(
        "mid-2",
        Str["id", "haven", "mid", "dis", "topRef"],
        Str:Str[]["id":Str["mid-2"], "topRef":Str["top-1", "top-2"]],
        Str:Str["dis":"Mid 2"],
        Str:Float[:],
        Str:Str?[:],
        Str:Bool[:],
        Str:Str[:],
        Str:Str[:],
        Str:Str[:],
        Str:Int[:]
      ))
  }

  private static Ref ref(Str str) { Ref.fromStr(str) }

  private TestData testData := TestData()
}

