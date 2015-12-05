package reactor.logging;

public interface IReactorLoggerFactory {

    /**
     * Return an appropriate {@link ReactorLogger} instance as specified by the
     * <code>name</code> parameter.

     * @param name the name of the Logger to return
     * @return a Logger instance
     */
    public ReactorLogger getLogger(String name);
}
