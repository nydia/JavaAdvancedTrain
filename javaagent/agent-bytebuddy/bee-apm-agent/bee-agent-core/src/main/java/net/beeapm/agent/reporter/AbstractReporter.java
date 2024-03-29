package net.beeapm.agent.reporter;


import net.beeapm.agent.model.Span;

import java.util.List;

/**
 * @author agent
 * @date 2022/08/05
 */
public abstract class AbstractReporter {
    private String name;

    public abstract int report(Span span);

    public abstract int report(List<Span> list);

    public abstract int init();
}
