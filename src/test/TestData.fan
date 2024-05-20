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

    // alpha
    Grid alpha := JsonReader(File(`test_data/alpha.json`).in).readVal
    alpha.each |d, i|
    {
      if (recs.containsKey(d->id))
        throw Err("oops ${d->id}")
      recs.add(d->id, d)
    }

    // TODO sys::IOErr: Unsupported JSON float literal: 'INF'
    // niagara
    f := File(`test_data/jason.txt`)
    f.eachLine |line|
    {
      Dict d := JsonReader(line.in).readVal

      if (recs.containsKey(d->id))
        throw Err("oops ${d->id}")
      recs.add(d->id, d)
    }

    // extra
    extra := Dict[
      Etc.dict2(
        "id", ref("z0"),
        "haven", M
      ),
      Etc.dict6(
        "id", ref("z1"),
        "haven", M,
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
      Etc.dict4(
        "id", ref("z2"),
        "haven", M,
        "a", n(1.0f),
        "b", Etc.dict3(
          "c", n(2.0f, "m"),
          "d", n(3),
          "e", n(4, "F")
        )
      ),
      Etc.dictFromMap([
        "id": ref("z3"),
        "haven": M,
        "a": true,
        "b": `https://project-haystack.org/`,
        "c": Date.fromStr("2021-03-22"),
        "d": Time.fromStr("17:19:23"),
        "e": DateTime.fromIso("2021-03-22T13:57:00.381-04:00")
      ]),
      Etc.dictFromMap([
        "id": ref("z4"),
        "haven": M,
        "b": `https://example.com/`,
      ]),
      Etc.dictFromMap([
        "id": ref("z5"),
        "haven": M,
        "b": "quux",
      ]),

      Etc.dict4("id", ref("str-1"), "haven", M, "str", "x", "nest", Etc.dict1("bar", "x")),
      Etc.dict4("id", ref("str-2"), "haven", M, "str", "y", "nest", Etc.dict1("bar", "y")),
      Etc.dict4("id", ref("str-3"), "haven", M, "str", "z", "nest", Etc.dict1("bar", "z")),

      Etc.dict3("id", ref("num-1"), "haven", M, "num", n(1)),
      Etc.dict3("id", ref("num-2"), "haven", M, "num", n(2)),
      Etc.dict3("id", ref("num-3"), "haven", M, "num", n(10)),
      Etc.dict3("id", ref("num-4"), "haven", M, "num", n(1,  "°F")),
      Etc.dict3("id", ref("num-5"), "haven", M, "num", n(2,  "°F")),
      Etc.dict3("id", ref("num-6"), "haven", M, "num", n(10, "°F")),
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

