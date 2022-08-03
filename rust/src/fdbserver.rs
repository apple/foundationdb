use std::pin::Pin;

#[cxx::bridge]
mod ffi {
    unsafe extern "C++" {
        // TODO: Does not work because this is an actor.h
     //   include!("fdbserver/ResolutionBalancer.actor.h"); // TODO: Use the .g?
     
     //   type ResolutionBalancer;
        // fn setResolvers(self: Pin<&mut ResolutionBalancer, ...);
    }
}
