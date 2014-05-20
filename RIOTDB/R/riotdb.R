## R functions exposed by the package and callable by the user
## Author: Yi Zhang
## Date: Sep 8, 2008
## Global ':' operator override: Mar 5, 2009
##############################################################

# patches will be made at the source level, instead of the script level
# newAssign <- function(x, value) {
#	if (class(value)=="dbvector"||class(value)=="dbmatrix") {
#		.Call("incRefCounter", value)
#		assign(deparse(substitute(x)),value,envir=parent.frame())
#	}
#	else
#		assign(deparse(substitute(x)),value,envir=parent.frame())
#}

# A vector created by `a:b' with length >= .VectorThreshold will be created
# as a dbvector
.VectorThreshold <- 1000

# setting up hooks when loading the package

.onAttach <- function(libname, pkgname) {
  cat("RIOTDB is a package that makes computing on large data I/O-efficient.\n")
  cat("It makes use of a database backend for storing and retrieving data.\n")
}

.onLoad = function(libname, pkgname) {
  ## generic methods for dbvector
  setClass("dbvector", representation(size="numeric",type="character",tablename="character",info="raw"))
  setMethod("show", signature("dbvector"), show.dbvector)
  setMethod("[", signature(x="dbvector"), function(x,i,j,...,drop=T){
    .Call("get_by_index", x, i)
  })
  setMethod("+", signature(e1="dbvector",e2="dbvector"), add_dbvectors)
  setMethod("-", signature(e1="dbvector",e2="dbvector"), subtract_dbvectors)
  setMethod("*", signature(e1="dbvector",e2="dbvector"), multiply_dbvectors)
  setMethod("/", signature(e1="dbvector",e2="dbvector"), divide_dbvectors)
  setMethod("Compare", signature(e1="dbvector",e2="numeric"), compare_dbvector_numeric)
  setMethod("Math", signature(x="dbvector"), math_dbvector)
  setMethod("sqrt", signature(x="dbvector"), sqrt_dbvector)
  setMethod("^", signature(e1="dbvector",e2="numeric"), pow_dbvector)
  setMethod("-", signature(e1="dbvector",e2="numeric"), subtract_dbvector_numeric)
  setMethod("max", signature(x="dbvector"), function(x,na.rm=FALSE){
    .Call("max_dbvector", x, na.rm)
  })
  setMethod("min", signature(x="dbvector"), function(x,na.rm=FALSE){
    .Call("min_dbvector", x, na.rm)
  })
  setMethod("sum", signature(x="dbvector"), function(x,na.rm=FALSE){
    .Call("sum_dbvector", x, na.rm)
  })

  setClass("dbmatrix", representation(size="numeric",type="character",tablename="character",info="raw"))
  setMethod("show", signature("dbmatrix"), show.dbmatrix)
  setMethod("dim", signature("dbmatrix"), function(x) {
    .Call("get_dim",x)})
  setMethod("%*%", signature(x="dbmatrix", y="dbmatrix"), function(x,y){
    if (dim(x)[2]!=dim(y)[1])
      stop("dimensions are not conformable")
    .Call("multiply_matrices", x, y)
  })
  setMethod("[", signature(x="dbmatrix"), function(x,i,j,...,drop=T){
    if (missing(i)||missing(j))
      stop("must provide two indexes")
    .Call("get_matrix_by_index", x, i, j)
  })
  setReplaceMethod("[", "dbvector", function(x,i,j,...,value){
    nas <- is.na(value)
    # if value contains only NA, delete instead
    if (all(nas)) {
      # currently i must be of numeric type, no dbvector!
      # sort index so that deletion can be done in ranges
      return(.Call("delete_from_dbvector", x, sort(i)))
    }
    # if some inside value are NA, first set then delete
    if (any(nas)) {
      entireNA <- rep(nas,length.out=length(i))
      toDel <- i[entireNA]
      y <- .Call("set", x, i, value)
      return(.Call("delete_from_dbvector", y, toDel))
    }
    # no NA's
    .Call("set", x, i, value)
  })

  # Ignore this.
  # overwrite the "<-" operator for the reference counting of db objects
  #	env <- as.environment('package:base')
  #	assign("oldAssign", base::`<-`, env)
  #	unlockBinding('<-', env)
  #	assign('<-', newAssign, envir=env)
  #	# Do NOT use lockBinding because its implementation calls '<-',
  #	# whose binding has not yet been locked!
  #	.Internal(lockBinding(as.name('<-'), env))

  ## Override the `:` operator in the base package to produce dbvector when
  ## the result is too large
  env <- as.environment('package:base')
  assign(".colon",base::`:`,env)
  unlockBinding(':',env)
  assign(':', colon ,env)
  lockBinding(as.name(':'),env)
}

colon <- function(x, y) {
  if (abs(x-y) < .VectorThreshold-1) return(.colon(x,y))
  else return(seq.db(x,y))
}

sqrt_dbvector <- function(x) {
  .Call("sqrt_dbvector", x)
}

pow_dbvector <- function(e1,e2) {
  .Call("pow_dbvector", e1, e2)
}

mean.dbvector <- function(x, na.rm=F) {
  .Call("mean_dbvector", x, na.rm)
}

compare_dbvector_numeric <- function(e1, e2) {
  # a hack for SQL
  if (.Generic == "==") .Generic = "="
  .Call("compare_dbvector_numeric", e1, e2, .Generic)
}

math_dbvector <- function(x) {
  switch (.Generic,
          sin=,
          cos= .Call("math_dbvector", x, .Generic)
          )
}

matrix.db <- function(nrows, ncols, from, to, by=1) {
  .Call("dbmatrix_from_seq", nrows, ncols, from, to, by)
}

matrix.db.vector <- function(from, nrows, ncols, byrow=FALSE) {
  if (class(from) != "dbvector")
    stop("the first input must be a dbvector")
  if (missing(nrows) && missing(ncols)) {
    nrows <- length(from)
    ncols <- 1
  } else if (missing(nrows)) {
    nrows <- length(from)/ncols
  } else if (missing(ncols)) {
    ncols <- length(from)/nrows
  }
  nrows <- as.integer(nrows)
  ncols <- as.integer(ncols)
  if (nrows*ncols != length(from))
    stop("dimension mismatch")
  .Call("dbmatrix_from_dbvector", from, nrows, ncols, byrow);
}

seq.db <- function(from=1, to=1, by=1) {
  if (missing(to) && missing(by)) {
    # copy the regular vector to a dbvector
    .Call("dbvector_copy", from)
  }
  else {
    # begin and end are always doubles
    if (!is.real(by)) stop("by should be an integer or real")
    if (to < from && missing(by)) by=-1
    if ((to < from && by >= 0) || (to > from && by <=0)) stop("wrong sign of by")

    .Call("dbvector_from_seq", from, to, by)
  }
}

complex.db <- function(length.out=0, real=numeric(), imaginary=numeric(),modulus=1,argument=0) {
  if (missing(modulus) && missing(argument))
    .Call("dbcomplex",length.out, real, imaginary, 0)
  else {
    n <- max(length.out, length(argument), length(modulus))
    temp <- rep(modulus,n)*exp((0+1i)*rep(argument,n))
    .Call("dbcomplex",n,Re(temp), Im(temp), 0)
  }
}

logical.db <- function(length=1, value=FALSE) {
  if (length < 1) stop("length must be at least 1")
  .Call("logical_db", length, value)
}

length.dbvector <- function(object) {
  .Call("length_dbvector", object)
}

show.dbvector <- function(object) {
  .Call("show_dbvector", object)
}

show.dbmatrix <- function(object) {
  .Call("show_dbmatrix", object)
}
add_dbvectors <- function(e1,e2) {
  .Call("add_dbvectors", e1, e2)
}
subtract_dbvectors <- function(e1,e2) {
  .Call("subtract_dbvectors", e1, e2)
}
multiply_dbvectors <- function(e1,e2) {
  .Call("multiply_dbvectors", e1, e2)
}
divide_dbvectors <- function(e1,e2) {
  .Call("divide_dbvectors", e1, e2)
}

sort.dbvector <- function(x, decreasing=FALSE) {
  .Call("dbvector_sort", x, decreasing)
}

load.dbvector <- function(name) {
  if (missing(name) || mode(name)!='numeric')
    stop("table metadata ID must be provided")
  .Call("load_dbvector", as.integer(name))
}

load.dbmatrix <- function(name) {
  if (missing(name) || mode(name)!='numeric')
    stop("table metadata ID must be provided")
  .Call("load_dbmatrix", as.integer(name))
}
persist <- function(x) {
  if (class(x)=="dbvector")
    .Call("persist_dbvector",x)
  else if (class(x)=="dbmatrix")
    .Call("persist_dbmatrix",x)
}

subtract_dbvector_numeric <- function(e1,e2) {
  if (length(e2) ==1)
    .Call("subtract_dbvector_numeric", e1, e2)
  else stop("second argument must be of length 1")
}

materialize <- function(x) {
  if (class(x)=="dbvector")
    .Call("materialize_dbvector", x)
  else if (class(x)=="dbmatrix")
    .Call("materialize_dbmatrix", x)
}

t.dbmatrix <- function(a) {
  .Call("do_mat_t", a)
}

solve.dbmatrix <- function(a, b) {
  if (dim(a)[1] != dim(a)[2])
    stop("the first input to solve() must be a square dbmatrix")
  if (missing(b)) {
    # Compute the inverse of a:
    .Call("do_mat_inv", a)
  } else {
    if (class(b) != "dbmatrix" || dim(a)[1] != dim(b)[1] || dim(b)[2] != 1)
      stop("the second input to solve(), if specified, must be a single-column dbmatrix with the same number of rows as the first input")
    .Call("do_mat_sol", a, b)
  }
}

print.db <- function(x, byrow=FALSE) {
  if (class(x) == "dbvector") {
    .Call("print_dbvector", x)
  } else if (class(x) == "dbmatrix") {
    .Call("print_dbmatrix", x, byrow)
  }
}
