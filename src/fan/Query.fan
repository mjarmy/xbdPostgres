//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

**
** Query is a haystack Filter that has been translated to parameterized SQL
**
internal const class Query
{
  **
  ** Only used for unit tests
  **
  internal new make(Str sql, Str:Obj params)
  {
    this.sql = sql
    this.params = params
  }

  **
  ** Create a Query from a Filter
  **
  internal static new fromFilter(Haven haven, Filter f, Bool isCount := false)
  {
     return QueryBuilder(haven, f).toQuery(isCount)
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
  internal const Str sql

  **
  ** The parameter name-to-value Map.
  **
  internal const Str:Obj params
}
