//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack
using haystack::Ref
using haystack::Dict
using xeto

**
** TestData
**
internal class TestData
{
  new make()
  {
    this.recs = initRecs
    this.xetoNs = initXeto
    this.context = TestContext(recs, xetoNs)
  }

  **
  ** initRecs
  **
  private static Ref:Dict initRecs()
  {
    recs := Ref:Dict[:]

    prefix := "../xetobase/xb-play/data/test/haven/"
    alphaFile := Env.cur.findFile(Uri(prefix + "alpha.json"))
    demogenFile := Env.cur.findFile(Uri(prefix + "demogen.zinc"))
    niagaraFile := Env.cur.findFile(Uri(prefix + "niagara.txt"))

    // alpha
    Grid alpha := JsonReader(alphaFile.in).readVal
    alpha.each |d, i|
    {
      if (recs.containsKey(d->id))
        throw Err("oops ${d->id}")
      recs.add(d->id, d)
    }

    // demogen
    Grid demogen := ZincReader(demogenFile.in).readVal
    demogen.each |d, i|
    {
      if (recs.containsKey(d->id))
        throw Err("oops ${d->id}")
      recs.add(d->id, d)
    }

    // niagara
    niagaraFile.eachLine |line|
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

      Etc.dict3("id", ref("bool-1"), "haven", M, "bool", true),
      Etc.dict3("id", ref("bool-2"), "haven", M, "bool", false),

      Etc.dict3("id", ref("date-1"), "haven", M, "foo", Date.fromStr("2021-03-22")),
      Etc.dict3("id", ref("date-2"), "haven", M, "foo", Date.fromStr("2021-03-23")),

      Etc.dict3("id", ref("time-1"), "haven", M, "bar", Time.fromStr("17:19:23")),
      Etc.dict3("id", ref("time-2"), "haven", M, "bar", Time.fromStr("17:19:24")),

      Etc.dict3("id", ref("dateTime-1"), "haven", M, "quux", DateTime.fromIso("2021-03-22T17:19:23.000-04:00")),
      Etc.dict3("id", ref("dateTime-2"), "haven", M, "quux", DateTime.fromIso("2021-03-23T17:19:24.000-04:00")),

      Etc.dict5("id", ref("top-1"), "haven", M, "top", M, "dis", "Top 1", "bogus", [ref("abc"), n(42)]),
      Etc.dict4("id", ref("top-2"), "haven", M, "top", M, "dis", "Top 2"),
      Etc.dict5("id", ref("mid-1"), "haven", M, "mid", M, "dis", "Mid 1", "topRef",  ref("top-1")),
      Etc.dict5("id", ref("mid-2"), "haven", M, "mid", M, "dis", "Mid 2", "topRef", [ref("top-1"), ref("top-2")]),
      Etc.dict5("id", ref("bot-1"), "haven", M, "bot", M, "dis", "Bot 1", "midRef",  ref("mid-1")),
      Etc.dict5("id", ref("bot-2"), "haven", M, "bot", M, "dis", "Bot 2", "midRef", [ref("mid-1"), ref("mid-2")])
    ]
    extra.each |d| { recs.add(d->id, d) }

    return recs
  }

  **
  ** initXeto
  **
  private static LibNamespace initXeto()
  {
    repo    := LibRepo.cur
    depends := repo.libs.map |n->LibDepend| { LibDepend(n) }
    vers    := repo.solveDepends(depends)
    ns      := repo.createNamespace(vers)
    return ns
  }

  **
  ** Run a filter against the recs
  **
  internal Dict[] filter(Filter f)
  {
    return recs.vals.findAll(|Dict r->Bool| { f.matches(r, context) })
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

  internal const Ref:Dict recs
  internal const LibNamespace xetoNs

  private HaystackContext context
}

**
** TestContext
**
internal class TestContext : HaystackContext
{
  new make(Ref:Dict recs, LibNamespace xetoNs)
  {
    this.recs = recs
    this.xetoNs = xetoNs
  }

  override Dict? deref(Ref id) { recs.get(id) }
  override FilterInference inference() { FilterInference.nil }
  override Dict toDict() { Etc.emptyDict }

  ** Return true if the given rec is nominally an instance of the given spec.
  override Bool xetoIsSpec(Str specName, xeto::Dict rec)
  {
    spec := specName.contains("::") ?
      xetoNs.type(specName) :
      xetoNs.unqualifiedType(specName)

    try
    {
      return xetoNs.specOf(rec).isa(spec)
    }
    catch (UnknownSpecErr e)
    {
      return false
    }
  }

  private const Ref:Dict recs
  private LibNamespace xetoNs
}
