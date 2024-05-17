//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

**
** TestData
**
const class TestData
{
  new make()
  {
    recs := Ref:Dict[:]

    // extra
    extra := Dict[
      Etc.dict1(
        "id", ref("z0")
      ),
      Etc.dict5(
        "id", ref("z1"),
        "a", "x",
        "b", Etc.dict3(
          "c", "y",
          "d", ref("z1"),
          "e", M
        ),
        "f", M,
        "g", Etc.dict1(
          "h", Etc.dict2(
            "i", Coord(37.0f, 77.0f),
            "j", "z"
          )
        )
      ),
      Etc.dict3(
        "id", ref("z2"),
        "a", n(1.0f),
        "b", Etc.dict3(
          "c", n(2.0f, "m"),
          "d", n(3),
          "e", n(4, "F")
        )
      ),
      Etc.dict6(
        "id", ref("z3"),
        "a", true,
        "b", `https://project-haystack.org/`,
        "c", Date.fromStr("2021-03-22"),
        "d", Time.fromStr("17:19:23"),
        "e", DateTime.fromIso("2021-03-22T13:57:00.381-04:00")
      ),
    ]
    extra.each |d| { recs.add(d->id, d) }

    this.recs = recs
  }

  // Run a filter against the recs
  Dict[] filter(Filter f)
  {
    res := Dict[,]

    pather := |Ref r->Dict?| { recs.get(r) }
    recs.each |r|
    {
      if (f.matches(r, PatherContext(pather)))
        res.add(r)
    }

    return res
  }

  private static Marker M() { Marker.val }
  private static Ref ref(Str str) { Ref.fromStr(str) }
  private static Number n(Num val, Str? unit := null)
  {
    Number.makeNum(val, unit == null ? null : Unit.fromStr(unit))
  }

  const Ref:Dict recs
}

