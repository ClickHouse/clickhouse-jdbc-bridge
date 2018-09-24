package ru.yandex.clickhouse.jdbcbridge.filter;

import ru.yandex.clickhouse.util.guava.StreamUtils;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import java.io.IOException;

/**
 * Created by krash on 24.09.18.
 */
public class DebugFilter implements Filter {
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        System.out.println(StreamUtils.toString(request.getInputStream()));
    }

    @Override
    public void destroy() {

    }
}
