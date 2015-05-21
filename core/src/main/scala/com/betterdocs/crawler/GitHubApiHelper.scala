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

package com.betterdocs.crawler

import java.io.File
import java.net.URL

import com.betterdocs.configuration.BetterDocsConfig
import com.betterdocs.crawler.GitHubRepoDownloader._
import com.betterdocs.logging.Logger
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.io.FileUtils
import org.eclipse.jgit.api.CloneCommand
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util.{Failure, Success, Try}

case class Repository(login: String, id: Int, name: String, fork: Boolean, language: String,
    defaultBranch: String, stargazersCount: Int)

/** For testing */
object Repository {
  def invalid: Repository = Repository("n-a", -1, "n-a", fork = false, "Java", "n-a", 0)
}

/**
 * This class relies on Github's {https://developer.github.com/v3/} Api.
 */
object GitHubApiHelper extends Logger {

  implicit val format = DefaultFormats
  private val client = new HttpClient()
  var token: String = BetterDocsConfig.githubTokens(0)

  /**
   * Access Github's
   * [[https://developer.github.com/v3/repos/#list-all-public-repositories List all repositories]]
   * @param since Specify id of repo to start the listing from. (Pagination)
   */
  def getAllGitHubRepos(since: Int): (List[Map[String, String]], Int) = {
    val method = executeMethod(s"https://api.github.com/repositories?since=$since", token)
    val nextSinceValueRaw = Option(method.getResponseHeader("Link").getElements.toList(0).getValue)
    val nextSince = nextSinceValueRaw.get.substring(0, nextSinceValueRaw.get.length - 1).toInt
    val json = httpGetJson(method).toList
    // Here we can specify all the fields we need from repo query.
    val interestingFields = List("full_name", "fork")
    val allGitHubRepos = for {
      j <- json
      c <- j.children
      map = (for {
        JObject(child) <- c
        JField(name, value) <- child
        if interestingFields.contains(name)
      } yield name -> value.values.toString).toMap
    } yield (map)
    (allGitHubRepos, nextSince)
  }

  /**
   * Get repository details for an organization.
   * Access Github's
   * [[https://developer.github.com/v3/repos/#list-organization-repositories]]
   */
  def getAllGitHubReposForOrg(orgs: String, page: Int): List[Repository] = {
    val method = executeMethod(s"https://api.github.com/orgs/$orgs/repos?page=$page", token)
    val json = httpGetJson(method).toList
    for (j <- json; c <- j.children) yield extractRepoInfo(c)
  }

  /**
   * Parallel fetch is not worth trying since github limits per user limit of 5000 Req/hr.
   */
  def fetchDetails(repoMap: Map[String, String]): Option[Repository] = {
    for {
      repo <-
      httpGetJson(executeMethod("https://api.github.com/repos/" + repoMap("full_name"), token))
    } yield extractRepoInfo(repo)
  }

  def extractRepoInfo(repo: JValue): Repository = {
    Repository((repo \ "owner" \ "login").extract[String],
      (repo \ "id").extract[Int], (repo \ "name").extract[String], (repo \ "fork").extract[Boolean],
      (repo \ "language").extract[String], (repo \ "default_branch").extract[String],
      (repo \ "stargazers_count").extract[Int])
  }

  /*
   * Find the number of repo pages in an organisation.
   */
  def repoPagesCount(url: String): Int = executeMethod(url, token).getResponseHeader("Link").
    getElements.toList(1).getValue.substring(0, 2).replaceAll("\\W+", "").toInt

  /*
     * Helper for accessing Java - Apache Http client. 
     * (It it important to stick with the current version and all.)
     */
  def httpGetJson(method: GetMethod): Option[JValue] = {
    val status = method.getStatusCode
    if (status == 200) {
      // ignored parsing errors if any, because we can not do anything about them anyway.
      Try(parse(method.getResponseBodyAsString)).toOption
    } else {
      log.error("Request failed with status:" + status + "Response:"
        + method.getResponseHeaders.mkString("\n") +
        "\nResponseBody " + method.getResponseBodyAsString)
      None
    }
  }

  def executeMethod(url: String, token: String): GetMethod = {
    println("Executing : " + url + " . with token: " + token)
    val method = new GetMethod(url)
    method.setDoAuthentication(true)
    // Please add the oauth token instead of <token> here. Or github may give 403/401 as response.
    method.addRequestHeader("Authorization", s"token $token")
    log.debug(s"using token $token")
    client.executeMethod(method)
    val requestLimitRemaining = method.getResponseHeader("X-RateLimit-Remaining").getValue
    repoDownloader ! RateLimit(requestLimitRemaining)
    method
  }

  def downloadRepository(r: Repository, targetDir: String): Option[File] = {
    try {
      val repoFile = new File(
        targetDir +
          s"/repo~${r.login}~${r.name}~${r.id}~${r.fork}~${r.language}~${r.defaultBranch}" +
          s"~${r.stargazersCount}.zip")
      log.info(s"Downloading $repoFile")
      FileUtils.copyURLToFile(new URL(
        s"https://github.com/${r.login}/${r.name}/archive/${r.defaultBranch}.zip"), repoFile)
      Some(repoFile)
    } catch {
      case x: Throwable =>
        log.error(s"Failed to download $r", x)
        None
    }
  }

   def cloneRepository(url: String, targetDir: String): Unit = {
    try {
      val file = new File(targetDir)
      val clone = new CloneCommand()
      clone.setBare(false)
      clone.setCloneAllBranches(true)
      clone.setDirectory(file).setURI(url)
      clone.call()
    } catch {
      case x: Throwable =>
        log.error(s"Failed to download $url", x)
    }
  }
}

object GitHubRepoCrawlerApp {

  import com.betterdocs.crawler.GitHubApiHelper._

  def main(args: Array[String]): Unit = {

    Try(args(0).toInt) match {

      case Success(since) =>
        log.info(s"Downloading repo since : $since")
        repoDownloader ! DownloadPublicRepos(since)

      case Failure(ex: NumberFormatException) if !args(0).isEmpty =>
        log.info(s"Downloading repo for organisation:" + args(0))
        repoDownloader ! DownloadOrganisationRepos(args(0))

    }
  }

  def downloadFromOrganization(organizationName: String): Unit = {
    val pageCount = repoPagesCount(s"https://api.github.com/orgs/$organizationName/repos")
    log.info("page count :" + pageCount)
    (1 to pageCount) foreach { page =>
      getAllGitHubReposForOrg(organizationName, page).filter(x => !x.fork && x.language == "Java")
        .map(x => downloadRepository(x, BetterDocsConfig.githubDir))
    }

  }

  def downloadFromRepoIdRange(since: Int): Int = {
    val (allGithubRepos, next) = getAllGitHubRepos(since)
    allGithubRepos.filter(x => x("fork") == "false").distinct
      .flatMap(fetchDetails).distinct.filter(x => x.language == "Java" && !x.fork)
      .map(x => downloadRepository(x, BetterDocsConfig.githubDir))
    next
  }
}
