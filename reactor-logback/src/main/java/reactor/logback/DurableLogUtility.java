/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.logback;

import ch.qos.logback.classic.BasicConfigurator;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleConfig;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.tools.ChronicleTools;
import org.apache.commons.cli.*;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

/**
 * @author Jon Brisbin
 */
public class DurableLogUtility {

	private static Options OPTS = new Options();

	static {
		Option path = new Option("p", "path", true, "Base path to a durable log file to interpret");
		path.setRequired(true);

		Option config = new Option("c", "config", true, "Logback configuration XML file to parse");
		Option output = new Option("o", "output", true, "Appender to use to output results");

		Option regex = new Option("search", true, "Search for the given regex and print to standard out");
		Option level = new Option("level", true, "Log level to filter");
		Option head = new Option("head", true, "Number of lines to display from the head of the file");
		Option tail = new Option("tail", true, "Number of lines to display from the tail of the file");
		OptionGroup findOpts = new OptionGroup()
				.addOption(regex)
				.addOption(head)
				.addOption(tail);

		OPTS.addOption(path)
		    .addOption(config)
		    .addOption(output)
		    .addOption(level)
		    .addOptionGroup(findOpts);
	}

	@SuppressWarnings("unchecked")
	public static void main(String... args) throws ParseException,
	                                               JoranException,
	                                               IOException {
		Parser parser = new BasicParser();
		CommandLine cl = null;
		try {
			cl = parser.parse(OPTS, args);
		} catch (ParseException e) {
			HelpFormatter help = new HelpFormatter();
			help.printHelp("dlog", OPTS, true);
			System.exit(-1);
		}

		LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
		loggerContext.reset();

		if (cl.hasOption("config")) {
			// Read Logback configuration
			JoranConfigurator configurator = new JoranConfigurator();
			configurator.setContext(loggerContext);

			configurator.doConfigure(cl.getOptionValue("file", "logback.xml"));

			StatusPrinter.printInCaseOfErrorsOrWarnings(loggerContext);
		} else {
			BasicConfigurator.configure(loggerContext);
		}

		Appender appender = null;
		if (cl.hasOption("output")) {
			String outputAppender = cl.getOptionValue("output", "console");
			appender = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME).getAppender(outputAppender);
		}

		ChronicleTools.warmup();
		Chronicle chronicle = new IndexedChronicle(cl.getOptionValue("path"), ChronicleConfig.DEFAULT);
		ExcerptTailer ex = chronicle.createTailer();

		Level level = Level.valueOf(cl.getOptionValue("level", "TRACE"));

		if (cl.hasOption("head")) {
			int lines = Integer.parseInt(cl.getOptionValue("head", "10"));
			for (int i = 0; i < lines; i++) {
				LoggingEvent evt = readLoggingEvent(ex, loggerContext);
				if (evt.getLevel().isGreaterOrEqual(level)) {
					writeEvent(evt, appender);
				}
			}
		} else if (cl.hasOption("tail")) {
			int lines = Integer.parseInt(cl.getOptionValue("tail", "10"));
			Queue<LoggingEvent> tail = new LinkedBlockingQueue<LoggingEvent>(lines);
			while (ex.nextIndex()) {
				LoggingEvent evt = readLoggingEvent(ex, loggerContext);
				if (!tail.offer(evt)) {
					tail.poll();
					tail.add(evt);
				}
			}
			LoggingEvent evt;
			while (null != (evt = tail.poll())) {
				if (evt.getLevel().isGreaterOrEqual(level)) {
					writeEvent(evt, appender);
				}
			}
		} else if (cl.hasOption("search")) {
			String regex = cl.getOptionValue("search");
			Pattern regexPatt = Pattern.compile(regex);
			while (ex.nextIndex()) {
				LoggingEvent evt = readLoggingEvent(ex, loggerContext);
				if (null != evt && evt.getLevel().isGreaterOrEqual(level)) {
					if (regexPatt.matcher(evt.getFormattedMessage()).matches()) {
						writeEvent(evt, appender);
					}
				}
			}
		}

		loggerContext.stop();
		chronicle.close();
	}

	@SuppressWarnings("unchecked")
	private static void writeEvent(LoggingEvent evt, Appender appender) {
		if (null == evt) {
			return;
		}
		if (null != appender) {
			appender.doAppend(evt);
		} else {
			System.out.println(evt.getFormattedMessage());
		}
	}

	@SuppressWarnings("unchecked")
	private static LoggingEvent readLoggingEvent(ExcerptTailer ex,
	                                             LoggerContext ctx) {
		LoggingEventRecord rec = LoggingEventRecord.read(ex);

		Logger logger = ctx.getLogger(rec.getLoggerName());
		LoggingEvent evt = new LoggingEvent(
				logger.getClass().getName(),
				logger,
				Level.toLevel(rec.getLevel()),
				rec.getMessage(),
				rec.getCause(),
				rec.getArgs()
		);
		evt.setTimeStamp(rec.getTimestamp());
		evt.setThreadName(rec.getThreadName());
		evt.setMDCPropertyMap(rec.getMdcProps());
		if (null != rec.getCause()) {
			evt.setThrowableProxy(new ThrowableProxy(rec.getCause()));
		}
		evt.setCallerData(rec.getCallerData());

		return evt;
	}

}
