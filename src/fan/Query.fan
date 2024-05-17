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

    sql := StrBuf()
    sql.add("select rec.brio from rec\n")
    for (i := 1; i <= qb.joins; i++)
    {
      prev := (i == 1) ? "rec" : "r${i-1}"
      sql.add("  inner join path_ref p$i on p${i}.source = ${prev}.id\n")
      sql.add("  inner join rec      r$i on r${i}.id     = p${i}.target\n")
    }
    sql.add("where\n")
    sql.add(qb.where)
    sql.add(";")

    return Query(sql.toStr, qb.params)
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

  override Str toStr() { "Query:\n$sql\nparams:$params" }

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
