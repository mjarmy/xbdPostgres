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

    this.recs = recs

    // make the records queryable
    queryable := Ref:Dict[:]
    recs.each |v,k| { queryable.add(k, makeQueryable(v)) }
    this.queryable = queryable
  }

  Dict[] filter(Filter f)
  {
    result := Dict[,]

    pather := |Ref r->Dict?| { recs.get(r) }
    recs.each |rec, id|
    {
      q := queryable.get(id)
      if (f.matches(q, PatherContext(pather)))
        result.add(rec)
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
  private const Ref:Dict queryable
}

