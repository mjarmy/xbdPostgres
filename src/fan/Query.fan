//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

**
** Query translates a haystack Filter to parameterized SQL
**
const class Query
{
  new make(Str sql, Str:Obj params)
  {
    this.sql = sql
    this.params = params
  }

  **
  ** Create a Query from a Filter
  **
  static new fromFilter(Filter f)
  {
    qb := QueryBuilder(f)

    ls := ["select rec.brio from rec"]
    for (i := 1; i <= qb.joins; i++)
    {
      prev := (i == 1) ? "rec" : "r${i-1}"
      ls.add("  inner join pathref p$i on p${i}.source = ${prev}.id")
      ls.add("  inner join rec     r$i on r${i}.id     = p${i}.target")
    }
    ls.add("where")
    ls.add("${qb.where};")

    return Query(ls.join("\n"), qb.params)
  }

//////////////////////////////////////////////////////////////////////////
// Obj
//////////////////////////////////////////////////////////////////////////

  override Int hash() { return sql.hash.xor(params.hash) }

  override Bool equals(Obj? that)
  {
    x := that as Query
    if (x == null) return false
    return sql == x.sql && params == x.params
  }

  override Str toStr() { "Query:\n$sql\nparams:$params\n" }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  **
  ** The parameterized SQL
  **
  const Str sql

  **
  ** The parameter name-to-value Map.
  **
  const Str:Obj params
}
