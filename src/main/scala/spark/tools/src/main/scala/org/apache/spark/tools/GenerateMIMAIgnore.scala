/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.tools

import java.io.File
import java.util.jar.JarFile

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.reflect.runtime.universe.runtimeMirror
import scala.reflect.runtime.{universe => unv}

/**
 * A tool for generating classes to be excluded during binary checking with MIMA. It is expected
 * that this tool is run with ./spark-class.
 *
 * MIMA itself only supports JVM-level visibility and doesn't account for package-private classes.
 * This tool looks at all currently package-private classes and generates exclusions for them. Note
 * that this approach is not sound. It can lead to false positives if we move or rename a previously
 * package-private class. It can lead to false negatives if someone explicitly makes a class
 * package-private that wasn't before. This exists only to help catch certain classes of changes
 * which might be difficult to catch during review.
 */
object GenerateMIMAIgnore {
  private val classLoader = Thread.currentThread().getContextClassLoader
  private val mirror = runtimeMirror(classLoader)


  private def isDeveloperApi(sym: unv.Symbol) =
    sym.annotations.exists(_.tpe =:= unv.typeOf[org.apache.spark.annotation.DeveloperApi])

  private def isExperimental(sym: unv.Symbol) =
    sym.annotations.exists(_.tpe =:= unv.typeOf[org.apache.spark.annotation.Experimental])


  private def isPackagePrivate(sym: unv.Symbol) =
    !sym.privateWithin.fullName.startsWith("<none>")

  private def isPackagePrivateModule(moduleSymbol: unv.ModuleSymbol) =
    !moduleSymbol.privateWithin.fullName.startsWith("<none>")

  /**
   * For every class checks via scala reflection if the class itself or contained members
   * have DeveloperApi or Experimental annotations or they are package private.
   * Returns the tuple of such classes and members.
   */
  private def privateWithin(packageName: String): (Set[String], Set[String]) = {

    val classes = getClasses(packageName)
    val ignoredClasses = mutable.HashSet[String]()
    val ignoredMembers = mutable.HashSet[String]()

    for (className <- classes) {
      try {
        val classSymbol = mirror.classSymbol(Class.forName(className, false, classLoader))
        val moduleSymbol = mirror.staticModule(className) // TODO: see if it is necessary.
        val directlyPrivateSpark =
          isPackagePrivate(classSymbol) || isPackagePrivateModule(moduleSymbol)
        val developerApi = isDeveloperApi(classSymbol)
        val experimental = isExperimental(classSymbol)

        /* Inner classes defined within a private[spark] class or object are effectively
         invisible, so we account for them as package private. */
        lazy val indirectlyPrivateSpark = {
          val maybeOuter = className.toString.takeWhile(_ != '$')
          if (maybeOuter != className) {
            isPackagePrivate(mirror.classSymbol(Class.forName(maybeOuter, false, classLoader))) ||
              isPackagePrivateModule(mirror.staticModule(maybeOuter))
          } else {
            false
          }
        }
        if (directlyPrivateSpark || indirectlyPrivateSpark || developerApi || experimental) {
          ignoredClasses += className
        } else {
          // check if this class has package-private/annotated members.
          ignoredMembers ++= getAnnotatedOrPackagePrivateMembers(classSymbol)
        }

      } catch {
        case _: Throwable => println("Error instrumenting class:" + className)
      }
    }
    (ignoredClasses.flatMap(c => Seq(c, c.replace("$", "#"))).toSet, ignoredMembers.toSet)
  }

  private def getAnnotatedOrPackagePrivateMembers(classSymbol: unv.ClassSymbol) = {
    classSymbol.typeSignature.members
      .filter(x => isPackagePrivate(x) || isDeveloperApi(x) || isExperimental(x)).map(_.fullName)
  }

  def main(args: Array[String]) {
    val (privateClasses, privateMembers) = privateWithin("org.apache.spark")
    scala.tools.nsc.io.File(".generated-mima-class-excludes").
      writeAll(privateClasses.mkString("\n"))
    println("Created : .generated-mima-class-excludes in current directory.")
    scala.tools.nsc.io.File(".generated-mima-member-excludes").
      writeAll(privateMembers.mkString("\n"))
    println("Created : .generated-mima-member-excludes in current directory.")
  }


  private def shouldExclude(name: String) = {
    // Heuristic to remove JVM classes that do not correspond to user-facing classes in Scala
    name.contains("anon") ||
    name.endsWith("$class") ||
    name.contains("$sp") ||
    name.contains("hive") ||
    name.contains("Hive")
  }

  /**
   * Scans all classes accessible from the context class loader which belong to the given package
   * and subpackages both from directories and jars present on the classpath.
   */
  private def getClasses(packageName: String): Set[String] = {
    val path = packageName.replace('.', '/')
    val resources = classLoader.getResources(path)

    val jars = resources.filter(x => x.getProtocol == "jar")
      .map(_.getFile.split(":")(1).split("!")(0)).toSeq

    jars.flatMap(getClassesFromJar(_, path))
      .map(_.getName)
      .filterNot(shouldExclude).toSet
  }

  /**
   * Get all classes in a package from a jar file.
   */
  private def getClassesFromJar(jarPath: String, packageName: String) = {
    import scala.collection.mutable
    val jar = new JarFile(new File(jarPath))
    val enums = jar.entries().map(_.getName).filter(_.startsWith(packageName))
    val classes = mutable.HashSet[Class[_]]()
    for (entry <- enums if entry.endsWith(".class")) {
      try {
        classes += Class.forName(entry.replace('/', '.').stripSuffix(".class"), false, classLoader)
      } catch {
        case _: Throwable => println("Unable to load:" + entry)
      }
    }
    classes
  }
}
