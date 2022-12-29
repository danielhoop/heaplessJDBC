#### .onLoad ####

.onLoad <- function (libname, pkgname) {
  ## Defining functions, only if they don't already exist ##
  thisNameSpace <- parent.env(environment())
  if(!exists("dir.exists")){
    dir.exists <- function(x) isTRUE(file.info(x)$isdir)
    assign("dir.exists", dir.exists, envir = thisNameSpace)
  }
  # vec <- c("ab^c", "bcd", NA, ""); str <- "ab^"; startsWith(vec, str); base::startsWith(vec, str)
  if (!exists("startsWith")) {
    startsWith <- function (x, prefix) {
      res <- substr(x, 1, nchar(prefix)) == prefix
      #res[is.na(res)] <- FALSE
      return (res)
    }
    assign("startsWith", startsWith, envir = thisNameSpace)
  }
  # vec <- c("abc", "bc^d", NA, ""); str <- "c^d"; endsWith(vec, str); base::endsWith(vec, str)
  if (!exists("endsWith")) {
    endsWith <- function (x, suffix) {
      nc <- nchar(x)
      res <- substr(x, nc-nchar(suffix)+1, nc) == suffix
      #res[is.na(res)] <- FALSE
      return (res)
    }
    assign("endsWith", endsWith, envir = thisNameSpace)
  }
}

#### Public ####
# Export these function (and the necessary private ones) into a GitLab package called 'heaplessJDBC'.

#' Create an object to hold the database settings.
#' @author Daniel Hoop
#' @export
#' @param driverPath The local path to the JDBC driver.
#' @param driverName The name of the driver.
#' @param address The URL of the database.
#' @param user The user name to login to the database.
#' @param password The password to login to the database.
#' @param javaBin The directory where the java binary file is located in case this directory is not given in the OS environment variable called \code{'PATH'}.
#'
#' @return A \code{list} of class \code{"dbSettings"} which holds the settings.
#' @examples
#' # Example for an ORACLE database
#' dbSettings(
#'   driverPath = "C:/drivers/database/ojdbc6.jar",
#'   driverName = "oracle.jdbc.driver.OracleDriver",
#'   address = "jdbc:oracle:thin:@//yourInternal.Domain.com:1521/", # 1521 is the an example port
#'   user = "yourDbUserName",
#'   password = "yourDbUserPassword",
#'   javaBin = "C:/Program Files/Java/jre8/bin")
dbSettings <- function (driverPath, driverName, address, user, password, javaBin = NULL) {

  exePath <- paste0(.libPaths(), "/heaplessJDBC/java/SQLToolbox.jar")
  exePath <- exePath[file.exists(exePath)]
  if (length(exePath) == 0)
    stop ("The dependency 'SQLToolbox.jar' could not be found. Please contact the package maintainer to report this error.")
  exePath <- exePath[1]

  res <- list(driverPath=driverPath, exePath=exePath, javaBin=javaBin,
              address=address, driverName=driverName, user=user, password=password)
  class(res) <- c(class(res), "dbSettings")
  return (res)
}

#' Execute a query and write result to file.
#' @author Daniel Hoop
#' @export
#' @param statement The SQL statement to execute.
#' @param outputFile The file to write the result to.
#' @param sep The column separator in the file
#' @param nullValue The value which will be written into the file, if the database value was NULL (not zero!).
#' @param dbSettings An object of class \code{"dbSettings"} which holds the settings for the database. Must be created with the function \code{\link{dbSettings}}.
#' @param checkOutputDir Logical value indicating if the function should check whether the output directory exists before executing the command.
#' @param showCmd Logical value indicating if the command sent to an internal CLI should be shown. Only for debugging purposes.
#' @return An error, if something went wrong, otherwise no return value.
#' @details See \code{\link{queryToDataFrame}} for an example.
queryToFile <- function(statement, outputFile, sep=";", nullValue="0", dbSettings, checkOutputDir=TRUE, showCmd=FALSE){

  if (!"dbSettings" %in% class(dbSettings))
    stop ("dbSettings must be an object of class 'dbSettings'.")
  d <- dbSettings
  a <- .prepareSqlToolboxCmd(statement=statement, outputFile=outputFile, sep=sep, nullValue=nullValue, driverPath=d$driverPath, exePath=d$exePath,
                             javaBin=d$javaBin, address=d$address, driverName=d$driverName, user=d$user, password=d$password, checkOutputDir=checkOutputDir)
  if(showCmd) cat(a$cmdTxt)

  response <- suppressWarnings(system(a$cmdTxt, intern=TRUE))
  if(a$delSql)
    file.remove(a$inputFile)
  if(any(grepl(".*java.*Exception", response[1:min(length(response), 10)]))){
    stop(paste0(response, collapse="\n"))
  }
  if(!file.exists(outputFile))
    stop ("The file was not created. It is not clear why.")
}

#' Execute a query and give back the result as a \code{data.frame}.
#' @author Daniel Hoop
#' @export
#' @inheritParams queryToFile
#' @return A \code{data.frame} which contains the result of the query.
#' @examples
#' # Make the settings
#' # Example for an ORACLE database
#' dbs <- dbSettings(
#'   driverPath = "C:/drivers/database/ojdbc6.jar",
#'   driverName = "oracle.jdbc.driver.OracleDriver",
#'   address = "jdbc:oracle:thin:@//yourInternal.Domain.com:1521/", # 1521 is the an example port
#'   user = "yourDbUserName",
#'   password = "yourDbUserPassword",
#'   javaBin = "C:/Program Files/Java/jre8/bin")
#'
#' # Execute
#' df <- queryToDataFrame(
#'   "SELECT * FROM persons WHERE age = 49",
#'   dbSettings = dbs)

queryToDataFrame <- function (statement, nullValue="0", dbSettings, showCmd=FALSE) {
  outputFile <- paste0(tempfile(pattern = "data_"), ".csv")
  sep <- ";"
  errorMsg <- tryCatch({
    queryToFile(statement, outputFile = outputFile, sep = sep, nullValue = nullValue, dbSettings = dbSettings, showCmd = showCmd)
    cols <- read.table(outputFile, sep = sep, header = FALSE, stringsAsFactors=FALSE, quote = "\"", comment.char = "", nrows = 1)
    data <- read.table(outputFile, sep = sep, header = FALSE, stringsAsFactors=FALSE, quote = "\"", comment.char = "", skip = 1)
    colnames(data) <- unlist(cols[1, ])
    if (file.exists(outputFile) && !file.remove(outputFile))
      warning("The temporary file written to disk could not be removed. It might expose sensitive information:\n", outputFile)
    "" # return
  }, error = function (e) {
    if (file.exists(outputFile) && !file.remove(outputFile))
      warning("The temporary file written to disk could not be removed. It might expose sensitive information:\n", outputFile)
    return (e$message)
  })
  if (errorMsg != "")
    stop (errorMsg)
  return (data)
}

# ALTE VERSION direkt mittels Konsole SIEHE UNTEN.
# SCHLECHTE PERFORMANCE!!!

# Performance-Vergleich
# system.time({
#   data <- dbUtilsZa::queryFromTemplateZa(
#     dbUtilsZa::getAllMmFromMetaDbZa(stichprobe = "spe")[300:1000], stichprobe = "spe", getWeights = TRUE)
# })

# Performance-Test mit hoher Netzwerk-Latenz
# Implementierung mit temporaerem File
#   User      System verstrichen
#   9.78        0.97       28.33
#
# Implementierung ueber Console
#   User      System verstrichen
#  63.72      122.61      205.44

# Performance-Testmit geringer Netzwerk-Latenz
# Auf dem RStudio Server
# Implementierung mit temporaerem File
#  user  system elapsed
# 6.921   0.690  17.403
#
# Implementierung ueber Console
#  user  system elapsed
# 8.332   0.625  18.488

# queryToDataFrame <- function(statement, nullValue="0", dbSettings, showCmd=FALSE){
#
#   if (!"dbSettings" %in% class(dbSettings))
#     stop ("dbSettings must be an object of class 'dbSettings'.")
#   d <- dbSettings
#   sep <- ";"
#   a <- .prepareSqlToolboxCmd(statement=statement, outputFile="", sep=sep, nullValue=nullValue, driverPath=d$driverPath, exePath=d$exePath,
#                              javaBin=d$javaBin, address=d$address, driverName=d$driverName, user=d$user, password=d$password, checkOutputDir=FALSE)
#   a$cmdTxt <- paste0(a$cmdTxt, " --writeToConsoleOnly")
#   if(showCmd) cat(a$cmdTxt)
#
#   # In case of an error use queryToFile because it will show the actual Exception.
#   tryCatch({
#     res <- system(a$cmdTxt, intern=TRUE)
#     if (!is.null(attributes(res)) && attributes(res)$status[1] == 1)
#       stop()
#   }, error=function(e){
#     queryToFile(statement=a$inputFile, outputFile="", sep=sep, nullValue=nullValue, dbSettings=d, checkOutputDir=FALSE)
#   }, warning=function(w){
#     queryToFile(statement=a$inputFile, outputFile="", sep=sep, nullValue=nullValue, dbSettings=d, checkOutputDir=FALSE)
#   })
#   h1 <- .removeQuotes(strsplit(res[1], sep)[[1]])
#   res <- char.cols.to.num(do.call("rbind", strsplit(res[-1], sep)))
#   res[] <- lapply(res, .removeQuotes)
#   colnames(res) <- h1
#   #invisible(gc())
#
#   if(a$delSql)
#     file.remove(a$inputFile)
#   #invisible(gc())
#   return(res)
# }





#### Private ####


.file.exists <- function (file) {
  if (is.null(file))
    return (FALSE)
  file.exists(file)
}


.prepareSqlToolboxCmd <- function (statement, outputFile, sep, nullValue, driverPath, exePath, javaBin=NULL, address, driverName, user, password, checkOutputDir=TRUE) {

  # This function prepares the cmd that is used to invoke the SQLToolbox main method in order to extract data from database.
  if (!is.null(javaBin) && !.file.exists(javaBin))
    stop ("The folder containing the java binary files does not exist (argument 'javaBin').")
  if (!.file.exists(exePath))
    stop ("The SQLToolbox jar does not exist (argument 'exePath').")
  if (!.file.exists(driverPath))
    stop ("The jar file containing the database driver does not exist (argument 'driverPath').")

  dirName <- dirname(outputFile)
  if (dirName == ".")
    outputFile <- paste0(getwd(), "/", basename(outputFile))
  if (checkOutputDir && !.file.exists(dirname(outputFile)))
    stop ("The directory to save the file to does not exist.")

  columnSeparator <- sep
  delSql <- FALSE

  if(.file.exists(statement)){
    inputFile <- statement
  } else {
    inputFile <- paste0(tempfile(pattern = "sqlQuery_"),".sql")
    write(statement, file=inputFile)
    delSql <- TRUE
  }

  .expand.Sys.env.path(javaBin)
  cmdTxt <- paste0("java -jar \"",exePath,"\" --driverPath \"",driverPath, "\" --driverName \"",driverName, "\" --address \"",address, "\" --user \"",user, "\" --password \"",password, "\" --inputFile \"",inputFile,"\" --outputFile \"",outputFile,"\" --columnSeparator \"",columnSeparator,"\" --nullValue \"",nullValue, "\" --replaceRegex \"\n\"")
  return(list(cmdTxt=cmdTxt, delSql=delSql, inputFile=inputFile))
}


# x <- c("asdf", "\"asf\"\"asdf\"")
.removeQuotes <- function(x) {
  if (!is.character(x))
    return (x)
  if (startsWith(x[1], "\"") && endsWith(x[1], "\"")) {
    x <- substr(x, 2, nchar(x)-1)
  }
  return (gsub("\"\"", "\"", x))
}

#### Dependencies ####

# Load necessary base functions, in case they are not already available.
# This can probably done automatically using the function "deparse"
# deparse(.expand.Sys.env.path) -> Then put it into another output-file.
.expand.Sys.env.path <- function(path){
  # This function checks if a path exists. If so, and the path is not yet part of the environment PATH variable, then the environment PATH will be expanded.
  # The function is needed, e.g. inside zip.nodirs()
  path <- path[!is.null(path)]
  path <- path[.file.exists(path)]
  if(length(path)>0) {
    PATH <- Sys.getenv("PATH")
    path <- .winSlashes(path)
    path <- path[ !path %in% unlist(strsplit(PATH, ";", fixed=TRUE)) ]
    if(length(path)>0)
      Sys.setenv(PATH=paste0(path, ";", PATH))
  }
}

# char.cols.to.num <- function(x, checkrowsForInteger=NULL, stringsAsFactors=FALSE){
#   # This function checks in all cols of a data.frame if they can be coerced to numeric without producing NA values.
#   # If it's possible the col is coerced to numeric with as.numeric()
#
#   if(is.matrix(x)) {
#     rownames(x) <- NULL
#     x <- as.data.frame(x, stringsAsFactors=stringsAsFactors)
#   }
#   if(is.vector(x)) {
#     names(x) <- NULL
#     x <- as.data.frame(as.list(x))
#   }
#   rn <- rownames(x)
#   cn <- colnames(x)
#
#   naT <- function(x){x[is.na(x)] <- TRUE; return(x)}
#   if(!is.null(checkrowsForInteger) && checkrowsForInteger>nrow(x)) checkrowsForInteger <- nrow(x)
#   x <- as.data.frame(lapply(x, function(x) if(is.character(x) || (!is.null(checkrowsForInteger) && all(naT(round(x[1:checkrowsForInteger])==x[1:checkrowsForInteger]))) ) type.convert(as.character(x),as.is=!stringsAsFactors) else x), stringsAsFactors=stringsAsFactors)
#   #x <- as.data.frame(lapply(x,function(x)if(is.character(x)) type.convert(x,as.is=!stringsAsFactors) else x), stringsAsFactors=stringsAsFactors)
#   #x <- as.data.frame(lapply(x, function(x) if( is( tryCatch(as.numeric(x[1:checkrowsForInteger]),error=function(e)e,warning=function(w)w), "warning") ) return(x) else return(as.numeric(x))    ), stringsAsFactors=stringsAsFactors)
#   rownames(x) <- rn
#   colnames(x) <- cn
#   #invisible(gc())
#   return(x)
# }

# replace.values <- function(search, replace, x, no.match.NA=FALSE, gsub=FALSE, fixed=FALSE){
#   # This function replaces all elements in argument search with the corresponding elements in argument replace.
#   # Replacement is done in x.
#   # if no.match.NA=TRUE, values that could not be replaced will be NA instead of the old value.
#
#   if(length(search)!=length(replace)) stop("length(search) must be equal length(replace)")
#   if(any(duplicated(search))) stop("There must be no duplicated entries in search.")
#
#   if(is.matrix(x)){
#     return(apply(x,2,function(x)replace.values(search=search, replace=replace, x=x, no.match.NA=no.match.NA, gsub=gsub, fixed=fixed)))
#   } else if(is.data.frame(x)){
#     return(as.data.frame(lapply(x,function(x)replace.values(search=search, replace=replace, x=x, no.match.NA=no.match.NA, gsub=gsub, fixed=fixed)),stringsAsFactors=FALSE))
#   }
#
#   if (is.factor(search) || is.factor(replace))
#     stop ("arguments 'search' and 'replace' must not contain factors.")
#   isFac <- is.factor(x)
#   if (isFac) {
#     lvl <- levels(x)
#     x <- as.character(x)
#   }
#
#   xnew <- x
#   if(!gsub) {
#     m1 <- match(x, search)
#     xnew <- replace[ m1 ]
#     if(!no.match.NA){
#       isna <- is.na(m1)
#       xnew[isna] <- x[isna]
#     }
#   } else {
#     # Hier erst nach Laenge der Strings ordnen, damit lange Teilstrings vor kurzen ersetzt werden.
#     ord <- order(nchar(search),decreasing=TRUE)
#     search <- search[ord]
#     replace <- replace[ord]
#     for(i in 1:length(search)){
#       xnew <- gsub(search[i],replace[i],xnew, fixed=fixed)
#     }
#   }
#   if (isFac) {
#     xnew <- factor(xnew, levels=lvl)
#   }
#   return(xnew)
# }

.winSlashes <- function(x, dontCheckOS=FALSE){
  # This function checks if the system is Windows. If so, then frontslashes are replaced with backslashes.

  if(any(is.na(x)))
    stop("None of paths must be NA.")
  if(!dontCheckOS && !grepl("window",Sys.info()["sysname"],ignore.case=TRUE))
    return(x)

  x <- gsub("/","\\\\",x)

  netDrive <- startsWith(x,"\\")
  x <- gsub("\\\\+","\\\\",x)
  x[netDrive] <- paste0("\\",x[netDrive])

  delEnd <- endsWith(x,"\\")
  x[delEnd] <- substr(x[delEnd], 1, nchar(x[delEnd])-1)
  return(x)
}