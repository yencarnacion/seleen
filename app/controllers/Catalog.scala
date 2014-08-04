package controllers

import play.api.mvc.{Action, Controller, Flash}
import play.api.data.Form
import play.api.data.Forms.{mapping, longNumber, nonEmptyText}
import play.api.i18n.Messages

object Catalog extends Controller {


  def index = Action {
    Ok(views.html.catalog.index())
  }


}