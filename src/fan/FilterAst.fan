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
internal const class FilterAst
{
  internal new make(Filter filter)
  {
    this.filter = filter

  }

  internal AstType type()
  {
    return visit(filter)
  }

  private AstType visit(Filter f)
  {

    switch (f.type)
    {
      case FilterType.has:
        return AstType.markers

      case FilterType.eq:
        return visitEq(f.argA, f.argB)

      default:
        return AstType.adHoc
    }
  }

  private AstType visitEq(FilterPath path, Obj arg)
  {
    if ((path.size == 1) && (arg is Ref))
      return AstType.refEq
    else
      return AstType.adHoc
  }

  internal const Filter filter
}

**************************************************************************
** AstType
**************************************************************************

internal enum class AstType
{
  markers,
  refEq,
  adHoc
}
