/*
 * IDispatched.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _IDISPATCHED_H_
#define _IDISPATCHED_H_

#pragma once

#include "flow/flow.h"

#include <map>

template <class T, typename K, typename F>
struct IDispatched {
	static std::map<K, F>& dispatches() {
		static std::map<K, F> theDispatches;
		return theDispatches;
	}
	static F const& dispatch(K k) {
		auto it = dispatches().find(k);
		if (it == dispatches().end())
			throw internal_error();
		return it->second;
	}
};

#define REGISTER_DISPATCHED(Type, Instance, Key, Func)                                                                 \
	struct Type##Instance {                                                                                            \
		Type##Instance() {                                                                                             \
			ASSERT(Type::dispatches().find(Key) == Type::dispatches().end());                                          \
			Type::dispatches()[Key] = Func;                                                                            \
		}                                                                                                              \
	};                                                                                                                 \
	Type##Instance _Type##Instance
#define REGISTER_DISPATCHED_ALIAS(Type, Instance, Target, Alias)                                                       \
	struct Type##Instance {                                                                                            \
		Type##Instance() {                                                                                             \
			ASSERT(Type::dispatches().find(Alias) == Type::dispatches().end());                                        \
			ASSERT(Type::dispatches().find(Target) != Type::dispatches().end());                                       \
			Type::dispatches()[Alias] = Type::dispatches()[Target];                                                    \
		}                                                                                                              \
	};                                                                                                                 \
	Type##Instance _Type##Instance;
#define REGISTER_COMMAND(Type, Instance, Key, Func) REGISTER_DISPATCHED(Type, Instance, Instance::Key, Instance::Func)

/*
  REGISTER_COMMAND is used for dispatching from type to static
  function. For example, to call one of multiple functions of
  (int, int) -> int, based on a string, you would write:

    struct BinaryArithmeticOp : IDispatched<BinaryArithmeticOp, std::string, std::function< int(int, int) >> {
        static int call( std::string const& op, int a, int b ) {
            return dispatch( op )( a, b );
        }
    };

    struct AddOp {
        static constexpr const char* opname = "+";
        static int call( int a, in b ) {
            return a + b;
        }
    };

  AddOp needs to be registered with REGISTER_COMMAND:

    REGISTER_COMMAND( BinaryArithmeticOp, AddOp, opname, call );

  If each BinaryArithmeticOp will have the same fields opname and
  call, it is probably easier to define a convenience macro:

    #define REGISTER_BINARY_OP(Op) REGISTER_COMMAND(BinaryArithmeticOp, Op, opname, call)

  The registration would then become:

    REGISTER_BINARY_OP( AddOp );

  Given std::string op, int x and int y, you would dynamically call
  the correct op with:

    BinaryArithmeticOp::call( op, x, y );
*/

#define REGISTER_FACTORY(Type, Instance, Key)                                                                          \
	REGISTER_DISPATCHED(Type, Instance, Instance::Key, Type::Factory<Instance>::create)

/*
  REGISTER_FACTORY is a formalized convention to simplify creating new
  or singleton objects that satisfy an interface, dispatched by a
  value of a given type. The type must have a nested, templated struct
  named Factory, which must have a method create that returns the
  desired type.

  For example:

    struct Message : IDispatched<Message, std::string, std::function< Message*(const char*) >>,
  ReferenceCounted<Message> { static Reference<Message> create( std::string const& message_type, const char* name ) {
            return Reference<Message>( dispatch( message_type )( name ) );
        }

        virtual std::string toString() = 0;

        template <class MessageType>
        struct Factory {
            static Message* create( const char* name ) {
                return (Message*)( new MessageType( name ) );
            }
        };
    };

    struct ExclaimMessage : Message {
        static constexpr const char* msgname = "exclaim";
        ExclaimMessage( const char* name ) : name(name) {}
        virtual std::string toString() {
            std::string s = format( "Hello, %s!", name );
        }
    };

  Each message must also be registered:

    REGISTER_FACTORY( Message, ExclaimMessage, msgname );

  This may also be shorted for a given type with another macro:

    #define REGISTER_MESSAGE(Msg) REGISTER_FACTOR(Message, Msg, msgname)

  And then be called as:

    REGISTER_MESSAGE( ExclaimMessage );

  Given std::string msgtype and const char* name, you would construct
  a new message with:

    Reference<Message> msg = Message::create( msgtype, name );
*/

#endif /* _IDISPATCHED_H_ */