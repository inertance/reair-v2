package com.airbnb.reair.common;


public interface Command<ReturnT, ExceptionT extends Throwable> {
  public ReturnT run() throws ExceptionT;
}
