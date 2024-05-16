//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

**
** TestData contains the records from the 'Alpha' data set from Project
** Haystack, and from a Niagara station that Jason helped put together.
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
      Etc.makeDict3("id", Ref.fromStr("x0"), "a", Number.makeInt(1), "b", Number.makeInt(1)),
      Etc.makeDict3("id", Ref.fromStr("x1"), "a", Number.makeInt(2), "b", Number.makeInt(2)),
      Etc.makeDict3("id", Ref.fromStr("x2"), "a", Number.makeInt(3), "b", Number.makeInt(3)),
      Etc.makeDict3("id", Ref.fromStr("x3"), "a", Number.makeInt(4), "b", Number.makeInt(4)),
      Etc.makeDict3("id", Ref.fromStr("x4"), "a", Number.makeInt(5), "c", Number.makeInt(1)),
      Etc.makeDict3("id", Ref.fromStr("x5"), "a", Number.makeInt(6), "c", Number.makeInt(2)),
      Etc.makeDict3("id", Ref.fromStr("x6"), "a", Number.makeInt(7), "c", Number.makeInt(3)),
      Etc.makeDict3("id", Ref.fromStr("x7"), "a", Number.makeInt(8), "c", Number.makeInt(4))
    ]
    extra.each |d| { recs.add(d->id, d) }

    // done
    this.recs = recs

    // make the records queryable
    queryable := Ref:Dict[:]
    recs.each |v,k| { queryable.add(k, makeQueryable(v)) }
    this.queryable = queryable
  }

  // Run a filter against the queryable recs
  Dict[] filter(Filter f)
  {
    result := Dict[,]

    pather := |Ref r->Dict?| { queryable.get(r) }
    queryable.each |rec, id|
    {
      q := queryable.get(id)
      if (f.matches(q, PatherContext(pather)))
        result.add(recs.get(id))
    }

    return result
  }

  // Make a rec queryable by stripping the units from numbers
  private static Dict makeQueryable(Dict d)
  {
    values := Str:Obj[:]
    d.each |v,k|
    {
      // traverseDict nested dict
      if (v is Dict)
      {
        values.add(k, makeQueryable(v))
      }
      // Number
      else if (v is Number)
      {
        n := (Number) v

        // Strip units
        if (n.unit != null)
        {
          n = n.isInt ?
            Number.makeInt(n.toInt) :
            Number(n.toFloat)
        }
        values.add(k, n)
      }
      // anything else
      else
      {
        values.add(k, v)
      }
    }
    return Etc.makeDict(values)
  }

  const Ref:Dict recs
  const Ref:Dict queryable
}

