package io.cresco.filerepo;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

import java.util.ArrayList;
import java.util.List;

public class Activator implements BundleActivator
{


    /**
     * Implements BundleActivator.start(). Prints
     * a message and adds itself to the bundle context as a service
     * listener.
     * @param context the framework context for the bundle.
     **/

    private List configurationList = new ArrayList();

    public void start(BundleContext context)
    {

        try {

        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Implements BundleActivator.stop(). Prints
     * a message and removes itself from the bundle context as a
     * service listener.
     * @param context the framework context for the bundle.
     **/
    public void stop(BundleContext context)
    {
        System.out.println("Stopped Bundle.");

    }

}