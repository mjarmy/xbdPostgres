//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   10 June 2024  Mike Jarmy  Creation
//

using haystack

**
** FilterAst
**
internal class FilterAst
{
  internal new make(Filter filter)
  {
    this.filter = filter
  }

  internal AstType type()
  {
    visit(filter)

    if (other > 0)
      return AstType.adHoc

    else if (markers > 0)
      return AstType.markers

    else if (refs == 1)
      return AstType.ref

    else if (specs == 1)
      return AstType.spec

    else
      return AstType.adHoc
  }

  private Void visit(Filter f)
  {
    switch (f.type)
    {
      // leafs
      case FilterType.has:
        markers++

      case FilterType.isSpec:
        specs++

      case FilterType.eq:
        if (((f.argA as FilterPath).size == 1) && (f.argB is Ref))
          refs++

      // compound
      case FilterType.and:
        visit(f.argA)
        visit(f.argB)

      // other
      default:
        other++
    }
  }

  private const Filter filter

  private Int markers := 0
  private Int refs    := 0
  private Int specs   := 0
  private Int other   := 0
}

**************************************************************************
** AstType
**************************************************************************

internal enum class AstType
{
  markers,
  ref,
  spec,
  adHoc
}
